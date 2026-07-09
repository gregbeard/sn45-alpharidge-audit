#!/usr/bin/env python3
"""
sn45 audit exporter (v2).

Produces per-epoch archive files consumed by the bittensor-free sidecar_audit.py.
This process is allowed to depend on bittensor and the alpharidge-ai package; the
sidecar is not. Everything here is STRICTLY READ-ONLY against the validator: it
reads the pm2 log files, the validator's JSON state files, and its own separate
Subtensor connection to the archive node. It never writes outside AUDIT_DATA_DIR.

Why log parsing: the live validator runs with API-served config that overrides
repo defaults (USD_PRICE_PER_POINT 0.0006 vs local default 0.04; reputation
gating ON vs default off), and the per-epoch reputation values that gate rewards
exist nowhere on disk in per-epoch form. The validator logs all of it every
settle cycle:
    [REMOTE_CONFIG] USD_PRICE_PER_POINT = 0.0006 (from API)
    [ValidationClient.run] Calculating weights and scores for target_epoch=85865
    [REWARDS] Local rewards: {156: 80, ...}
    [REWARDS] Broadcasted rewards: {13: 26, ...}
    [REWARDS] UID=2 hk=5FnwS55kYwoT.. gated=165 (rep=0.703 mult=1.000 vol=165)
    [calculate_weights] total_percent_needed=8.9081%, rewards_count=34
    [calculate_weights] Non-zero weights: [(2, np.float64(0.0033246757)), ...]... (burn_uid=189, burn_weight=0.9109)
Those lines are the validator's own claims; the archive stores them as inputs and
"validator_reported" values, and the sidecar independently re-does the arithmetic.

Run with the alpharidge-ai venv interpreter:
    /home/rizzo/sn45/alpharidge-ai/.venv/bin/python export_epoch_archives.py

On-chain asymmetry: the chain only exposes the LATEST weight vector per
validator (each set_weights overwrites the last), so on-chain ground truth is
captured live when metagraph.last_update advances. Chain-state INPUTS (subnet
price, emission, min/max weight limits) are fetched at the exact historical
settlement block via the archive node. The external TAO/USD price has no
historical source; archives record the fetch-time value flagged accordingly.
"""
from __future__ import annotations

import argparse
import ast
import gzip
import json
import logging
import os
import re
import sys
import time
import traceback
from pathlib import Path

def _load_dotenv(path: Path) -> None:
    """Minimal .env loader (KEY=VALUE lines; real env vars win)."""
    try:
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    except FileNotFoundError:
        pass


_BASE = Path(__file__).resolve().parent.parent
_load_dotenv(_BASE / ".env")

# Required per-deployment settings — no defaults, so another validator can't
# accidentally run this against the wrong hotkey or a missing node.
_missing = [k for k in ("ALPHARIDGE_REPO", "ARCHIVE_SUBTENSOR_ENDPOINT", "VALIDATOR_HOTKEY_SS58")
            if not os.environ.get(k)]
if _missing:
    sys.exit(f"[exporter] Missing required config (set in {_BASE / '.env'} or environment): {', '.join(_missing)}")

ALPHARIDGE_REPO = Path(os.environ["ALPHARIDGE_REPO"])
if not (ALPHARIDGE_REPO / "alpharidge_ai").is_dir():
    sys.exit(f"[exporter] ALPHARIDGE_REPO={ALPHARIDGE_REPO} does not contain the alpharidge_ai package")
sys.path.insert(0, str(ALPHARIDGE_REPO))

import numpy as np  # noqa: E402
from bittensor.core.subtensor import Subtensor  # noqa: E402
import requests  # noqa: E402

from alpharidge_ai import config as ar_config  # noqa: E402
from alpharidge_ai.validator import reputation as reputation_mod  # noqa: E402
from alpharidge_ai.utils import burn as burn_mod  # noqa: E402
from alpharidge_ai.base.utils import weight_utils  # noqa: E402
from alpharidge_ai.models.reward import Reward  # noqa: E402

NETUID = int(os.environ.get("NETUID", "45"))
SCHEMA_VERSION = 2
ARCHIVE_ENDPOINT = os.environ["ARCHIVE_SUBTENSOR_ENDPOINT"]
VALIDATOR_HOTKEY_SS58 = os.environ["VALIDATOR_HOTKEY_SS58"]
BASE_DIR = _BASE
DATA_DIR = Path(os.environ.get("AUDIT_DATA_DIR", str(BASE_DIR / "data" / "epoch_archives")))
STATE_PATH = Path(os.environ.get("EXPORTER_STATE_PATH", str(BASE_DIR / "data" / "exporter_state.json")))
PM2_LOG_DIR = Path(os.environ.get("PM2_LOG_DIR", str(Path.home() / ".pm2" / "logs")))
VALIDATOR_LOG_PREFIX = os.environ.get("VALIDATOR_LOG_PREFIX", "alpharidge.validator")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "120"))
SETTLEMENT_LAG_EPOCHS = 2
KEEP_EPOCH_RECORDS = 200

logging.basicConfig(level=logging.INFO, format="%(asctime)s [exporter] %(levelname)s %(message)s")
log = logging.getLogger("exporter")

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
RE_TARGET_EPOCH = re.compile(r"Calculating weights and scores for target_epoch=(\d+)")
RE_LOCAL_REWARDS = re.compile(r"\[REWARDS\] Local rewards: (\{.*\})")
RE_BROADCAST_REWARDS = re.compile(r"\[REWARDS\] Broadcasted rewards: (\{.*\})")
RE_GATED = re.compile(
    r"\[REWARDS\] UID=(\d+) hk=(\S+)\.\. gated=(\d+) \(rep=([\d.]+) mult=([\d.]+) vol=(\d+)\)"
)
RE_APPLYING = re.compile(r"\[REWARDS\] Applying reward for UID=(\d+) hotkey=(\S+)\.\.\. \(points=(\d+) > penalties=(\d+)\)")
RE_ZEROING = re.compile(r"\[PENALTIES\] Zeroing reward for UID=(\d+) hotkey=(\S+)\.\.\. \(points=(\d+) <= penalties=(\d+)\)")
RE_CALC_TOTAL = re.compile(r"\[calculate_weights\] total_percent_needed=([\d.]+)%, rewards_count=(\d+)")
RE_CALC_WEIGHTS = re.compile(
    r"\[calculate_weights\] Non-zero weights: \[(.*?)\]\.\.\. \(burn_uid=(\d+), burn_weight=([\d.]+)\)"
)
RE_CALC_WEIGHT_PAIR = re.compile(r"\((\d+), np\.float64\(([\deE.+-]+)\)\)")
RE_REMOTE_CONFIG = re.compile(r"\[REMOTE_CONFIG\] (\w+) = (.+?) \((from API|local OVERRIDE)\)")
RE_BLACKLIST_IGNORE = re.compile(r"Ignoring broadcast rewards for blacklisted UID=(\d+)")


def archive_path(epoch: int) -> Path:
    return DATA_DIR / f"epoch_{epoch:08d}.json.gz"


def atomic_write_json_gz(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True, default=str)
    tmp.rename(path)


def read_json(path: Path, default):
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Log scanning
# ---------------------------------------------------------------------------
class LogScanner:
    """Incrementally parse the validator's pm2 logs into per-epoch records.

    State survives restarts and log rotation via STATE_PATH. Rotated files are
    parsed exactly once; live files are tailed by byte offset (offset resets if
    the file shrank, i.e. copytruncate-style rotation)."""

    def __init__(self, state: dict):
        self.state = state
        self.state.setdefault("live_offsets", {})       # path -> offset
        self.state.setdefault("processed_rotated", [])  # list of file names
        self.state.setdefault("epochs", {})             # epoch(str) -> record
        self.state.setdefault("remote_config", {})      # key -> {value, source, ts}
        # target_epoch context persists across chunks of the same live file
        self.state.setdefault("epoch_context", {})      # path -> last seen target_epoch

    def epoch_record(self, epoch: int) -> dict:
        rec = self.state["epochs"].setdefault(str(epoch), {})
        rec.setdefault("local_rewards", {})
        rec.setdefault("broadcast_rewards", {})
        rec.setdefault("gated", {})
        rec.setdefault("applied", {})
        rec.setdefault("zeroed", {})
        rec.setdefault("blacklist_ignored_uids", [])
        rec.setdefault("validator_reported", {})
        return rec

    def parse_text(self, text: str, path_key: str, mtime: float) -> None:
        current_epoch = self.state["epoch_context"].get(path_key)
        for raw_line in text.splitlines():
            line = ANSI_RE.sub("", raw_line)

            m = RE_TARGET_EPOCH.search(line)
            if m:
                current_epoch = int(m.group(1))
                continue

            m = RE_REMOTE_CONFIG.search(line)
            if m:
                key, value, source = m.group(1), m.group(2).strip(), m.group(3)
                self.state["remote_config"][key] = {"value": value, "source": source, "ts": mtime}
                continue

            if current_epoch is None:
                continue

            m = RE_LOCAL_REWARDS.search(line)
            if m:
                try:
                    parsed = ast.literal_eval(m.group(1))
                    rec = self.epoch_record(current_epoch)
                    rec["local_rewards"] = {str(k): int(v) for k, v in parsed.items()}
                except Exception:
                    pass
                continue

            m = RE_BROADCAST_REWARDS.search(line)
            if m:
                try:
                    parsed = ast.literal_eval(m.group(1))
                    rec = self.epoch_record(current_epoch)
                    rec["broadcast_rewards"] = {str(k): int(v) for k, v in parsed.items()}
                except Exception:
                    pass
                continue

            m = RE_GATED.search(line)
            if m:
                uid, hk_prefix, gated, rep, mult, vol = m.groups()
                rec = self.epoch_record(current_epoch)
                rec["gated"][uid] = {
                    "gated": int(gated), "rep": float(rep), "mult": float(mult),
                    "vol": int(vol), "hk_prefix": hk_prefix,
                }
                continue

            m = RE_APPLYING.search(line)
            if m:
                uid, hk_prefix, pts, pen = m.groups()
                rec = self.epoch_record(current_epoch)
                rec["applied"][uid] = {"points": int(pts), "penalties": int(pen), "hk_prefix": hk_prefix}
                continue

            m = RE_ZEROING.search(line)
            if m:
                uid, hk_prefix, pts, pen = m.groups()
                rec = self.epoch_record(current_epoch)
                rec["zeroed"][uid] = {"points": int(pts), "penalties": int(pen), "hk_prefix": hk_prefix}
                continue

            m = RE_BLACKLIST_IGNORE.search(line)
            if m:
                rec = self.epoch_record(current_epoch)
                uid = int(m.group(1))
                if uid not in rec["blacklist_ignored_uids"]:
                    rec["blacklist_ignored_uids"].append(uid)
                continue

            m = RE_CALC_TOTAL.search(line)
            if m:
                rec = self.epoch_record(current_epoch)
                rec["validator_reported"]["total_percent_needed"] = float(m.group(1))
                rec["validator_reported"]["rewards_count"] = int(m.group(2))
                continue

            m = RE_CALC_WEIGHTS.search(line)
            if m:
                rec = self.epoch_record(current_epoch)
                pairs = {u: float(w) for u, w in RE_CALC_WEIGHT_PAIR.findall(m.group(1))}
                rec["validator_reported"]["sample_weights"] = pairs
                rec["validator_reported"]["burn_uid"] = int(m.group(2))
                rec["validator_reported"]["burn_weight"] = float(m.group(3))
                continue

        self.state["epoch_context"][path_key] = current_epoch

    def scan(self) -> None:
        live_files, rotated_files = [], []
        for suffix in ("-error", "-out"):
            live = PM2_LOG_DIR / f"{VALIDATOR_LOG_PREFIX}{suffix}.log"
            if live.exists():
                live_files.append(live)
            rotated_files.extend(sorted(PM2_LOG_DIR.glob(f"{VALIDATOR_LOG_PREFIX}{suffix}__*.log")))

        for path in rotated_files:
            if path.name in self.state["processed_rotated"]:
                continue
            try:
                text = path.read_text(errors="replace")
                self.parse_text(text, path_key=path.name, mtime=path.stat().st_mtime)
                self.state["processed_rotated"].append(path.name)
                log.info(f"Parsed rotated log {path.name} ({len(text)//1024} KiB)")
            except Exception:
                log.warning(f"Failed parsing {path}:\n{traceback.format_exc()}")

        for path in live_files:
            key = str(path)
            try:
                size = path.stat().st_size
                offset = self.state["live_offsets"].get(key, 0)
                if size < offset:
                    offset = 0  # truncated by rotation
                    self.state["epoch_context"].pop(key, None)
                if size == offset:
                    continue
                with open(path, "r", errors="replace") as f:
                    f.seek(offset)
                    chunk = f.read()
                self.parse_text(chunk, path_key=key, mtime=path.stat().st_mtime)
                self.state["live_offsets"][key] = offset + len(chunk.encode("utf-8", errors="replace"))
            except Exception:
                log.warning(f"Failed tailing {path}:\n{traceback.format_exc()}")

        # bound state growth
        epochs = sorted(self.state["epochs"], key=int)
        for e in epochs[:-KEEP_EPOCH_RECORDS]:
            del self.state["epochs"][e]

    def served_config(self) -> dict:
        return {k: v["value"] for k, v in self.state["remote_config"].items()}


# ---------------------------------------------------------------------------
# Exporter
# ---------------------------------------------------------------------------
class Exporter:
    def __init__(self):
        log.info(f"Connecting to archive subtensor at {ARCHIVE_ENDPOINT}")
        self.sub = Subtensor(network=ARCHIVE_ENDPOINT)
        self.state = read_json(STATE_PATH, {})
        self.scanner = LogScanner(self.state)
        self.validator_uid: int | None = None

    def save_state(self) -> None:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state))
        tmp.rename(STATE_PATH)

    # -- external price (no historical source; flagged in the archive) -------
    def tao_price_usd_best_effort(self) -> float | None:
        api_url = getattr(ar_config, "MINER_API_URL", None)
        if not api_url or api_url == "null":
            return None
        try:
            resp = requests.get(f"{api_url.rstrip('/')}/price/tao-usd", timeout=10)
            resp.raise_for_status()
            return float(resp.json()["price_usd"])
        except Exception as e:
            log.warning(f"tao_price_usd fetch failed: {e}")
            return None

    # -- typed view of the served (API) config with local-default fallback ---
    def effective_config(self) -> dict:
        served = self.scanner.served_config()

        def get(key, cast, default):
            if key in served:
                try:
                    if cast is bool:
                        return str(served[key]).strip().lower() in ("1", "true", "yes", "on")
                    return cast(served[key])
                except (ValueError, TypeError):
                    pass
            return default

        return {
            "USD_PRICE_PER_POINT": get("USD_PRICE_PER_POINT", float, ar_config.USD_PRICE_PER_POINT),
            "MIN_PERCENT_PER_POINT": get("MIN_PERCENT_PER_POINT", float, ar_config.MIN_PERCENT_PER_POINT),
            "EPOCH_LENGTH": ar_config.EPOCH_LENGTH,  # not remote-served (absent from _REMOTE_CONFIG_KEYS)
            "BLOCK_LENGTH": ar_config.BLOCK_LENGTH,
            "BURN_UID": ar_config.BURN_UID,
            "REPUTATION_GATING_ENABLED": get("REPUTATION_GATING_ENABLED", bool,
                                             bool(getattr(ar_config, "REPUTATION_GATING_ENABLED", False))),
            "EMISSION_MIDPOINT": get("EMISSION_MIDPOINT", float, getattr(ar_config, "EMISSION_MIDPOINT", 0.59)),
            "EMISSION_GAIN": get("EMISSION_GAIN", float, getattr(ar_config, "EMISSION_GAIN", 100.0)),
            "EMISSION_BONUS_CEILING": get("EMISSION_BONUS_CEILING", float, getattr(ar_config, "EMISSION_BONUS_CEILING", 0.0)),
            "EMISSION_BONUS_START": get("EMISSION_BONUS_START", float, getattr(ar_config, "EMISSION_BONUS_START", 0.63)),
            "EMISSION_BONUS_FULL": get("EMISSION_BONUS_FULL", float, getattr(ar_config, "EMISSION_BONUS_FULL", 0.75)),
            "_served_raw": served,
        }

    # -- validator state files (read-only snapshots for cross-checks) --------
    @staticmethod
    def store_snapshots(target_epoch: int) -> dict:
        out = {}
        rs = read_json(Path(ar_config.REWARD_STORE_LOCATION), {})
        out["reward_store_epoch"] = (rs.get("epoch_rewards") or {}).get(str(target_epoch))
        ps = read_json(Path(ar_config.PENALTY_STORE_LOCATION), {})
        out["penalty_store_epoch"] = (ps.get("epoch_penalties") or {}).get(str(target_epoch))
        bs = read_json(Path(ar_config.BROADCAST_STATE_LOCATION), {})
        out["reward_broadcasts_epoch"] = (bs.get("by_epoch_by_sender") or {}).get(str(target_epoch))
        pbs = read_json(Path(ar_config.PENALTY_BROADCAST_STATE_LOCATION), {})
        out["penalty_broadcasts_epoch"] = (pbs.get("by_epoch_by_sender") or {}).get(str(target_epoch))
        rep = read_json(ALPHARIDGE_REPO / "alpharidge_ai" / ".reputation_state.json", {})
        out["reputation_state_current"] = {
            hk: st for hk, st in (rep.get("state") or {}).items()
        }
        return out

    # ------------------------------------------------------------------
    def build_archive(self, target_epoch: int, onchain_snapshot: dict | None) -> dict | None:
        rec = self.state["epochs"].get(str(target_epoch))
        if not rec or (not rec.get("gated") and not rec.get("applied")):
            log.debug(f"No settle data in logs yet for epoch {target_epoch}; skipping")
            return None

        eff = self.effective_config()
        settlement_block = (target_epoch + 1) * eff["BLOCK_LENGTH"] - 1

        metagraph = self.sub.metagraph(NETUID, lite=False, block=settlement_block)
        hotkeys = list(metagraph.hotkeys)

        # chain state pinned at the historical settlement block
        min_allowed_weights = self.sub.min_allowed_weights(netuid=NETUID, block=settlement_block)
        max_weight_limit = self.sub.max_weight_limit(netuid=NETUID, block=settlement_block)
        subnet_price_tao = float(self.sub.get_subnet_price(netuid=NETUID, block=settlement_block).tao)
        emission_raw = self.sub.query_module(
            "SubtensorModule", "SubnetAlphaOutEmission", [NETUID], block=settlement_block
        )
        emission_rao = int(getattr(emission_raw, "value", emission_raw))
        tao_usd = self.tao_price_usd_best_effort()

        # rewards list exactly as the validator built it (gated path logs every uid;
        # non-gated path logs applied/zeroed)
        rewards_by_uid: dict[int, int] = {}
        rep_inputs: dict[str, dict] = {}
        if eff["REPUTATION_GATING_ENABLED"] and rec.get("gated"):
            for uid, g in rec["gated"].items():
                rewards_by_uid[int(uid)] = int(g["gated"])
                rep_inputs[uid] = g
        else:
            for uid, a in rec.get("applied", {}).items():
                rewards_by_uid[int(uid)] = int(a["points"])
            for uid in rec.get("zeroed", {}):
                rewards_by_uid[int(uid)] = 0

        rewards_list = []
        for uid, val in rewards_by_uid.items():
            if 0 <= uid < len(hotkeys):
                rewards_list.append(Reward(hotkey=hotkeys[uid], reward=val, epoch=target_epoch))

        # --- reference replay: the REAL production calculate_weights() with its
        # external inputs pinned (module-level monkey-patch inside our process
        # only; the repo on disk is untouched) ---
        alpha_per_point_value = (subnet_price_tao * tao_usd / eff["USD_PRICE_PER_POINT"]) if tao_usd else 0.0
        miner_alpha_per_block_value = (emission_rao * (1 - 0.18) * 0.5) / 10**9

        saved = (burn_mod.get_alpha_per_point, burn_mod.get_miner_alpha_per_block,
                 ar_config.MIN_PERCENT_PER_POINT, ar_config.USD_PRICE_PER_POINT)
        burn_mod.get_alpha_per_point = lambda: alpha_per_point_value
        burn_mod.get_miner_alpha_per_block = lambda: miner_alpha_per_block_value
        ar_config.MIN_PERCENT_PER_POINT = eff["MIN_PERCENT_PER_POINT"]
        ar_config.USD_PRICE_PER_POINT = eff["USD_PRICE_PER_POINT"]
        try:
            reference_weights = burn_mod.calculate_weights(rewards_list, metagraph)
        finally:
            (burn_mod.get_alpha_per_point, burn_mod.get_miner_alpha_per_block,
             ar_config.MIN_PERCENT_PER_POINT, ar_config.USD_PRICE_PER_POINT) = saved

        norm = np.linalg.norm(reference_weights, ord=1)
        raw_weights = (reference_weights / norm) if norm > 0 and not np.isnan(norm) \
            else np.ones_like(reference_weights) / len(reference_weights)

        class _FrozenSub:
            def min_allowed_weights(self, netuid): return min_allowed_weights
            def max_weight_limit(self, netuid): return max_weight_limit

        processed_uids, processed_weights = weight_utils.process_weights_for_netuid(
            uids=np.arange(len(hotkeys)), weights=raw_weights, netuid=NETUID,
            subtensor=_FrozenSub(), metagraph=metagraph,
        )
        emit_uids, emit_vals = weight_utils.convert_weights_and_uids_for_emit(processed_uids, processed_weights)

        return {
            "schema_version": SCHEMA_VERSION,
            "netuid": NETUID,
            "epoch": target_epoch,
            "block_length": eff["BLOCK_LENGTH"],
            "settlement_block": settlement_block,
            "exported_at_block": self.sub.get_current_block(),
            "exported_at_unix": time.time(),
            "validator_hotkey": VALIDATOR_HOTKEY_SS58,
            "inputs": {
                "local_rewards": rec.get("local_rewards", {}),
                "broadcast_rewards": rec.get("broadcast_rewards", {}),
                "gated": rec.get("gated", {}),
                "applied": rec.get("applied", {}),
                "zeroed": rec.get("zeroed", {}),
                "blacklist_ignored_uids": rec.get("blacklist_ignored_uids", []),
                "rewards_by_uid": {str(u): v for u, v in rewards_by_uid.items()},
                "hotkeys_by_uid": {str(u): hotkeys[u] for u in rewards_by_uid if 0 <= u < len(hotkeys)},
                "metagraph_n": len(hotkeys),
            },
            "store_snapshots": self.store_snapshots(target_epoch),
            "config": {k: v for k, v in self.effective_config().items() if k != "_served_raw"},
            "config_served_raw": self.effective_config()["_served_raw"],
            "chain_state": {
                "min_allowed_weights": min_allowed_weights,
                "max_weight_limit": max_weight_limit,
                "subnet_price_tao": subnet_price_tao,
                "tao_price_usd": tao_usd,
                "tao_price_usd_is_historical": False,
                "subnet_alpha_out_emission_per_block_rao": emission_rao,
                "alpha_per_point": alpha_per_point_value,
                "miner_alpha_per_block": miner_alpha_per_block_value,
                "queried_block": settlement_block,
            },
            "validator_reported": rec.get("validator_reported", {}),
            "reference": {
                "scores": {str(i): float(w) for i, w in enumerate(reference_weights) if w > 0},
                "raw_weights": {str(i): float(w) for i, w in enumerate(raw_weights) if w > 0},
                "processed_weights": {str(int(u)): float(w) for u, w in zip(processed_uids, processed_weights) if w > 0},
                "uint16_weights": {str(int(u)): int(v) for u, v in zip(emit_uids, emit_vals)},
            },
            "onchain": onchain_snapshot or {"status": "not_captured"},
        }

    # ------------------------------------------------------------------
    def check_onchain_transition(self) -> tuple[int, dict] | None:
        metagraph = self.sub.metagraph(NETUID, lite=False)
        if self.validator_uid is None:
            if VALIDATOR_HOTKEY_SS58 not in metagraph.hotkeys:
                log.warning("Validator hotkey not found in current metagraph")
                return None
            self.validator_uid = metagraph.hotkeys.index(VALIDATOR_HOTKEY_SS58)
            log.info(f"Validator uid resolved to {self.validator_uid}")

        last_update_block = int(metagraph.last_update[self.validator_uid])
        prev = self.state.get("last_seen_update_block")
        if last_update_block <= 0 or last_update_block == prev:
            return None
        self.state["last_seen_update_block"] = last_update_block

        observed_epoch = last_update_block // ar_config.BLOCK_LENGTH - SETTLEMENT_LAG_EPOCHS
        weights_row = metagraph.weights[self.validator_uid]
        snapshot = {
            "status": "captured_live",
            "last_update_block": last_update_block,
            "observed_epoch_estimate": observed_epoch,
            "weights_fraction": {str(uid): float(w) for uid, w in enumerate(weights_row) if w > 0},
            "captured_at_unix": time.time(),
        }
        return observed_epoch, snapshot

    # ------------------------------------------------------------------
    def run_once(self):
        self.scanner.scan()

        onchain = self.check_onchain_transition()
        onchain_epoch = None
        if onchain is not None:
            onchain_epoch, snapshot = onchain
            log.info(f"Detected new on-chain weights -> epoch {onchain_epoch}")
            try:
                archive = self.build_archive(onchain_epoch, snapshot)
                if archive:
                    atomic_write_json_gz(archive_path(onchain_epoch), archive)
                    log.info(f"Wrote archive for epoch {onchain_epoch} (with live on-chain ground truth)")
            except Exception:
                log.error(f"Failed live-triggered archive for epoch {onchain_epoch}:\n{traceback.format_exc()}")

        # Backfill every log-observed epoch that has no archive yet (or was
        # written without on-chain data and still has none — no overwrite there,
        # a later live capture wins).
        for epoch_str in sorted(self.state["epochs"], key=int):
            e = int(epoch_str)
            if e == onchain_epoch or archive_path(e).exists():
                continue
            try:
                archive = self.build_archive(e, onchain_snapshot=None)
                if archive:
                    atomic_write_json_gz(archive_path(e), archive)
                    log.info(f"Backfilled archive for epoch {e} (no on-chain ground truth)")
            except Exception:
                log.warning(f"Skipping backfill for epoch {e}:\n{traceback.format_exc()}")

        # keep an index.json alongside the archives so the directory can be
        # served over HTTP for the sidecar's remote-URL mode
        try:
            files = sorted(p.name for p in DATA_DIR.glob("epoch_*.json.gz"))
            (DATA_DIR / "index.json").write_text(json.dumps({"files": files}))
        except Exception:
            pass

        self.save_state()

    def run_forever(self):
        while True:
            try:
                self.run_once()
            except Exception:
                log.error(f"Export cycle failed:\n{traceback.format_exc()}")
            time.sleep(POLL_INTERVAL_SECONDS)


def main():
    parser = argparse.ArgumentParser(description="sn45 audit epoch archive exporter")
    parser.add_argument("--once", action="store_true", help="run a single export cycle and exit")
    args = parser.parse_args()

    exporter = Exporter()
    if args.once:
        exporter.run_once()
    else:
        exporter.run_forever()


if __name__ == "__main__":
    main()
