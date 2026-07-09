#!/usr/bin/env python3
"""
SN45 AlphaRidge Auditor — standalone audit sidecar.

A single-file, stdlib-only process (NO bittensor, NO LLM, NO third-party
dependencies) that arithmetically verifies the sn45 validator's scoring and
weight-setting from per-epoch archive files produced by
exporter/export_epoch_archives.py.

Per epoch it independently replays and checks:
  1. gate_replay          reward_i == round(emission(rep_i) * volume_i)
                          (reputation-gated path), or the points>penalties rule
                          on the non-gated path
  2. weight_replay        the alpha-economics weight formula:
                          percent_i = max((r_i/alpha_per_point)/total_alpha*100,
                                          r_i*MIN_PERCENT_PER_POINT),
                          scale to <=100%, weight_i = percent_i*scale/100,
                          burn = 1 - min(total,100)/100  -- vs archive reference
  3. validator_reported   replayed weights vs the totals/weights the validator
                          itself logged (tolerance covers TAO-price cache drift)
  4. uint16_replay        L1-normalize -> max-weight clip -> max-upscale ->
                          round(w*65535) -- vs archive reference
  5. onchain_match        replayed weight fractions vs the weights actually
                          observed on chain (when the exporter captured them)
  6. dust_budget          every unscored/zero-reward miner has exactly zero
                          weight; the only remainder sink is the burn UID and
                          it equals the burn formula exactly
  7. store_consistency    validator log claims vs its on-disk reward store

Serves an HTML dashboard + /api/state + /health on AUDIT_PORT (default 18889)
using ThreadingHTTPServer with a 15s per-handler timeout.

Archive source: AUDIT_ARCHIVE_SOURCE -- a local directory (default
./data/epoch_archives) or a public http(s) base URL serving the same
epoch_*.json.gz files (with an index.json {"files": [...]} or an HTML listing).
"""
from __future__ import annotations

import gzip
import io
import json
import math
import os
import re
import threading
import time
import traceback
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

VERSION = "1.0.0"


def _load_dotenv(path: Path) -> None:
    """Minimal stdlib .env loader (KEY=VALUE lines; real env vars win)."""
    try:
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    except FileNotFoundError:
        pass


_load_dotenv(Path(__file__).resolve().parent / ".env")

PORT = int(os.environ.get("AUDIT_PORT", "18889"))
BIND = os.environ.get("AUDIT_BIND", "0.0.0.0")
ARCHIVE_SOURCE = os.environ.get(
    "AUDIT_ARCHIVE_SOURCE",
    str(Path(__file__).resolve().parent / "data" / "epoch_archives"),
)
RELOAD_INTERVAL_S = int(os.environ.get("AUDIT_RELOAD_INTERVAL_S", "60"))
# Relative tolerance for checks whose inputs include the non-historical TAO/USD
# price (the validator uses a 5-min cache; the exporter fetches later).
PRICE_DRIFT_TOL = float(os.environ.get("AUDIT_PRICE_DRIFT_TOL", "0.05"))
MAX_EPOCHS = int(os.environ.get("AUDIT_MAX_EPOCHS", "500"))
# An epoch settles every ~20 min; if the newest archive is older than this the
# pipeline (exporter, validator logging, archive node) has stalled.
STALE_AFTER_S = int(os.environ.get("AUDIT_STALE_AFTER_S", "3600"))
U16_MAX = 65535

PASS, FAIL, WARN, SKIP = "PASS", "FAIL", "WARN", "SKIP"


# ---------------------------------------------------------------------------
# Replayed arithmetic (mirrors alpharidge_ai/validator/reputation.py and
# alpharidge_ai/utils/burn.py + base/utils/weight_utils.py, reimplemented in
# pure stdlib so this file shares no code with the system under audit)
# ---------------------------------------------------------------------------
def gate(reputation: float, midpoint: float, gain: float) -> float:
    return 1.0 / (1.0 + math.exp(-gain * (reputation - midpoint)))


def emission(reputation: float, midpoint: float, gain: float,
             bonus_ceiling: float, bonus_start: float, bonus_full: float) -> float:
    floor = gate(reputation, midpoint, gain)
    denom = bonus_full - bonus_start
    if denom <= 1e-9:
        ramp = 1.0 if reputation >= bonus_start else 0.0
    else:
        ramp = min(1.0, max(0.0, (reputation - bonus_start) / denom))
    return floor * (1.0 + bonus_ceiling * ramp)


def replay_weights(rewards_by_uid: dict[int, int], n: int, chain: dict, cfg: dict):
    """calculate_weights() replayed from archived inputs. Returns (weights list,
    total_percent_needed)."""
    app = float(chain.get("alpha_per_point") or 0.0)
    mapb = float(chain.get("miner_alpha_per_block") or 0.0)
    total_alpha = mapb * float(cfg["EPOCH_LENGTH"])
    min_ppp = float(cfg["MIN_PERCENT_PER_POINT"])

    percent = {}
    total = 0.0
    for uid, r in rewards_by_uid.items():
        econ = ((r / app) / total_alpha * 100.0) if (app > 0 and total_alpha > 0) else 0.0
        p = max(econ, r * min_ppp)
        percent[uid] = p
        total += p

    weights = [0.0] * n
    scale = (100.0 / total) if total > 100.0 else 1.0
    for uid, p in percent.items():
        if 0 <= uid < n:
            weights[uid] = p * scale / 100.0
    burn_uid = int(cfg["BURN_UID"])
    if 0 <= burn_uid < n:
        weights[burn_uid] = 1.0 - (min(total, 100.0) / 100.0)
    return weights, total


def normalize_max_weight(x: list[float], limit: float) -> list[float]:
    """Pure-python port of the vendored normalize_max_weight()."""
    s = sum(x)
    n = len(x)
    if s == 0 or n * limit <= 1:
        return [1.0 / n] * n
    estimation = sorted(v / s for v in x)
    if estimation[-1] <= limit:
        return [v / s for v in x]
    epsilon = 1e-7
    cumsum = []
    acc = 0.0
    for v in estimation:
        acc += v
        cumsum.append(acc)
    n_values = 0
    for i, v in enumerate(estimation):
        estimation_sum_i = (n - i - 1) * v
        if v / (estimation_sum_i + cumsum[i] + epsilon) < limit:
            n_values += 1
    cutoff_scale = (limit * cumsum[n_values - 1] - epsilon) / (1 - (limit * (n - n_values)))
    cutoff = cutoff_scale * sum(x)
    clipped = [min(v, cutoff) for v in x]
    cs = sum(clipped)
    return [v / cs for v in clipped]


def convert_for_emit(uids: list[int], weights: list[float]):
    """Pure-python port of convert_weights_and_uids_for_emit()."""
    if not weights or sum(weights) == 0:
        return [], []
    mx = max(weights)
    scaled = [w / mx for w in weights]
    out_uids, out_vals = [], []
    for uid, w in zip(uids, scaled):
        v = round(w * U16_MAX)
        if v != 0:
            out_uids.append(uid)
            out_vals.append(v)
    return out_uids, out_vals


def process_and_emit(weights: list[float], min_allowed: int, max_limit: float):
    """L1-normalize -> process_weights_for_netuid -> uint16, replayed."""
    n = len(weights)
    norm = sum(abs(w) for w in weights)
    raw = [w / norm for w in weights] if norm > 0 else [1.0 / n] * n

    nz = [(i, w) for i, w in enumerate(raw) if w > 0]
    if not nz or n < min_allowed:
        final = [1.0 / n] * n
        return list(range(n)), final, "uniform_fallback"
    if len(nz) < min_allowed:
        w2 = [1e-5] * n
        for i, w in nz:
            w2[i] += w
        normed = normalize_max_weight(w2, max_limit)
        return list(range(n)), normed, "epsilon_fill"
    # exclude_quantile is 0 at the live call site -> lowest_quantile = min(nonzero),
    # which drops nothing
    uids = [i for i, _ in nz]
    vals = normalize_max_weight([w for _, w in nz], max_limit)
    return uids, vals, "normal"


# ---------------------------------------------------------------------------
# Archive loading (local dir or remote URL)
# ---------------------------------------------------------------------------
def _read_gz_json(data: bytes) -> dict:
    return json.loads(gzip.GzipFile(fileobj=io.BytesIO(data)).read().decode("utf-8"))


def list_and_load_archives(source: str) -> dict[int, dict]:
    archives: dict[int, dict] = {}
    if source.startswith(("http://", "https://")):
        base = source.rstrip("/")
        names: list[str] = []
        try:
            with urllib.request.urlopen(f"{base}/index.json", timeout=15) as r:
                names = json.loads(r.read()).get("files", [])
        except Exception:
            try:
                with urllib.request.urlopen(base + "/", timeout=15) as r:
                    html = r.read().decode("utf-8", errors="replace")
                names = sorted(set(re.findall(r"epoch_\d+\.json\.gz", html)))
            except Exception:
                return archives
        for name in sorted(names)[-MAX_EPOCHS:]:
            try:
                with urllib.request.urlopen(f"{base}/{name}", timeout=30) as r:
                    a = _read_gz_json(r.read())
                archives[int(a["epoch"])] = a
            except Exception:
                continue
    else:
        d = Path(source)
        for p in sorted(d.glob("epoch_*.json.gz"))[-MAX_EPOCHS:]:
            try:
                a = _read_gz_json(p.read_bytes())
                archives[int(a["epoch"])] = a
            except Exception:
                continue
    return archives


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------
def check(status: str, detail: str) -> dict:
    return {"status": status, "detail": detail}


def audit_epoch(a: dict) -> dict:
    cfg = a["config"]
    chain = a["chain_state"]
    inputs = a["inputs"]
    ref = a.get("reference", {})
    n = int(inputs.get("metagraph_n") or 256)
    rewards_by_uid = {int(u): int(v) for u, v in (inputs.get("rewards_by_uid") or {}).items()}
    checks: dict[str, dict] = {}

    # -- 1. gate replay ------------------------------------------------------
    gated = inputs.get("gated") or {}
    if cfg.get("REPUTATION_GATING_ENABLED") and gated:
        bad, edge = [], 0
        for uid, g in gated.items():
            mult = emission(float(g["rep"]), cfg["EMISSION_MIDPOINT"], cfg["EMISSION_GAIN"],
                            cfg["EMISSION_BONUS_CEILING"], cfg["EMISSION_BONUS_START"],
                            cfg["EMISSION_BONUS_FULL"])
            expect = int(round(mult * int(g["vol"])))
            got = int(g["gated"])
            if expect != got:
                if abs(expect - got) <= 1:
                    edge += 1  # logged rep is rounded to 3 decimals; ±1 point is print-precision
                else:
                    bad.append(f"uid {uid}: expected {expect}, stored {got} (rep={g['rep']}, vol={g['vol']})")
        if bad:
            checks["gate_replay"] = check(FAIL, f"{len(bad)}/{len(gated)} mismatched: " + "; ".join(bad[:5]))
        else:
            msg = f"all {len(gated)} miners: round(emission(rep)*vol) == stored reward"
            if edge:
                msg += f" ({edge} within ±1 from 3-decimal rep logging)"
            checks["gate_replay"] = check(PASS, msg)
    elif cfg.get("REPUTATION_GATING_ENABLED"):
        checks["gate_replay"] = check(SKIP, "gating enabled but no per-miner gate inputs in archive")
    else:
        applied, zeroed, bad = inputs.get("applied") or {}, inputs.get("zeroed") or {}, []
        for uid, r in applied.items():
            if not (int(r["points"]) > int(r["penalties"])):
                bad.append(f"uid {uid} applied but points<=penalties")
        for uid, r in zeroed.items():
            if int(r["points"]) > int(r["penalties"]):
                bad.append(f"uid {uid} zeroed but points>penalties")
        checks["gate_replay"] = check(FAIL, "; ".join(bad[:5])) if bad else check(
            PASS, f"points>penalties rule holds for {len(applied)} applied / {len(zeroed)} zeroed")

    # -- 2. weight replay vs archive reference -------------------------------
    weights, total_pct = replay_weights(rewards_by_uid, n, chain, cfg)
    ref_scores = {int(u): float(w) for u, w in (ref.get("scores") or {}).items()}
    diffs = []
    for uid in set(list(ref_scores) + [i for i, w in enumerate(weights) if w > 0]):
        rw, aw = weights[uid] if uid < n else 0.0, ref_scores.get(uid, 0.0)
        if abs(rw - aw) > max(1e-9, 1e-6 * max(abs(rw), abs(aw))):
            diffs.append(f"uid {uid}: replay {rw:.9f} vs reference {aw:.9f}")
    checks["weight_replay"] = check(FAIL, f"{len(diffs)} uids differ: " + "; ".join(diffs[:5])) if diffs \
        else check(PASS, f"replayed formula reproduces reference for all uids (total={total_pct:.4f}%)")

    # -- 3. vs validator-reported --------------------------------------------
    vr = a.get("validator_reported") or {}
    implied_price = None
    if vr.get("total_percent_needed") is not None and total_pct > 0:
        rep_total = float(vr["total_percent_needed"])
        rel = abs(rep_total - total_pct) / max(rep_total, 1e-12)
        if chain.get("tao_price_usd"):
            implied_price = float(chain["tao_price_usd"]) * total_pct / rep_total
        sample_bad = []
        for uid, w in (vr.get("sample_weights") or {}).items():
            rw = weights[int(uid)] if int(uid) < n else 0.0
            if abs(rw - float(w)) > PRICE_DRIFT_TOL * max(float(w), 1e-12):
                sample_bad.append(f"uid {uid}: replay {rw:.7f} vs logged {float(w):.7f}")
        burn_note = ""
        if vr.get("burn_weight") is not None:
            burn_replay = weights[int(cfg["BURN_UID"])]
            if abs(burn_replay - float(vr["burn_weight"])) > PRICE_DRIFT_TOL:
                sample_bad.append(f"burn: replay {burn_replay:.4f} vs logged {vr['burn_weight']:.4f}")
            else:
                burn_note = f"; burn {burn_replay:.4f} vs logged {vr['burn_weight']:.4f}"
        if rel > PRICE_DRIFT_TOL or sample_bad:
            checks["validator_reported"] = check(
                FAIL, f"total {total_pct:.4f}% vs logged {rep_total:.4f}% (rel {rel:.2%}); " + "; ".join(sample_bad[:5]))
        else:
            checks["validator_reported"] = check(
                PASS, f"total {total_pct:.4f}% vs logged {rep_total:.4f}% (rel {rel:.2%}, within TAO-price drift tol){burn_note}")
    else:
        checks["validator_reported"] = check(SKIP, "no validator-logged totals in archive")

    # -- 4. uint16 replay -----------------------------------------------------
    ref_u16 = {int(u): int(v) for u, v in (ref.get("uint16_weights") or {}).items()}
    if ref_u16:
        uids, vals, mode = process_and_emit(weights,
                                            int(chain.get("min_allowed_weights") or 0),
                                            float(chain.get("max_weight_limit") or 1.0))
        e_uids, e_vals = convert_for_emit(uids, vals)
        mine = dict(zip(e_uids, e_vals))
        bad = [f"uid {u}: replay {mine.get(u, 0)} vs reference {ref_u16.get(u, 0)}"
               for u in set(mine) | set(ref_u16) if abs(mine.get(u, 0) - ref_u16.get(u, 0)) > 1]
        checks["uint16_replay"] = check(FAIL, f"{len(bad)} uids differ: " + "; ".join(bad[:5])) if bad \
            else check(PASS, f"uint16 emit payload reproduced ({len(ref_u16)} uids, path={mode})")
    else:
        checks["uint16_replay"] = check(SKIP, "no uint16 reference in archive")

    # -- 5. on-chain match ----------------------------------------------------
    onchain = a.get("onchain") or {}
    if onchain.get("status") == "captured_live" and onchain.get("weights_fraction"):
        oc = {int(u): float(w) for u, w in onchain["weights_fraction"].items()}
        s = sum(weights)
        frac = {i: w / s for i, w in enumerate(weights) if w > 0} if s > 0 else {}
        bad = [f"uid {u}: onchain {oc.get(u, 0):.6f} vs replay {frac.get(u, 0):.6f}"
               for u in set(oc) | set(frac)
               if abs(oc.get(u, 0.0) - frac.get(u, 0.0)) > max(2e-4, PRICE_DRIFT_TOL * frac.get(u, 0.0))]
        checks["onchain_match"] = check(FAIL, f"{len(bad)} uids differ: " + "; ".join(bad[:5])) if bad \
            else check(PASS, f"on-chain weights match replay for {len(oc)} uids")
    else:
        checks["onchain_match"] = check(SKIP, "no live on-chain capture for this epoch (chain only exposes latest weights)")

    # -- 6. dust / epsilon budget ---------------------------------------------
    burn_uid = int(cfg["BURN_UID"])
    burn_expect = 1.0 - (min(total_pct, 100.0) / 100.0)
    problems = []
    if abs(weights[burn_uid] - burn_expect) > 1e-12:
        problems.append(f"burn {weights[burn_uid]:.9f} != 1-min(total,100)/100 = {burn_expect:.9f}")
    scored = {u for u, r in rewards_by_uid.items() if r > 0}
    leaked = [i for i, w in enumerate(weights) if w > 0 and i not in scored and i != burn_uid]
    if leaked:
        problems.append(f"nonzero weight for unscored uids {leaked[:10]}")
    zeroed_scored = [u for u in rewards_by_uid if rewards_by_uid[u] == 0 and weights[u] != 0]
    if zeroed_scored:
        problems.append(f"zero-reward miners with nonzero weight: {zeroed_scored[:10]}")
    checks["dust_budget"] = check(FAIL, "; ".join(problems)) if problems else check(
        PASS, f"no dust: unscored miners all zero-weight; burn absorbs remainder exactly ({burn_expect:.4f})")

    # -- 7. store consistency ---------------------------------------------------
    store_epoch = (a.get("store_snapshots") or {}).get("reward_store_epoch")
    local_logged = {int(u): int(v) for u, v in (inputs.get("local_rewards") or {}).items()}
    hk_by_uid = {int(u): hk for u, hk in (inputs.get("hotkeys_by_uid") or {}).items()}
    if store_epoch and local_logged and hk_by_uid:
        bad = []
        for uid, pts in local_logged.items():
            hk = hk_by_uid.get(uid)
            if hk is None:
                continue
            store_pts = store_epoch.get(hk)
            if store_pts is not None and int(store_pts) != pts:
                bad.append(f"uid {uid}: log {pts} vs store {store_pts}")
        checks["store_consistency"] = check(WARN, "; ".join(bad[:5])) if bad else check(
            PASS, f"validator-logged local points match on-disk reward store ({len(local_logged)} miners)")
    else:
        checks["store_consistency"] = check(SKIP, "store snapshot pruned or unavailable for this epoch")

    statuses = [c["status"] for c in checks.values()]
    overall = FAIL if FAIL in statuses else (WARN if WARN in statuses else PASS)

    return {
        "epoch": a["epoch"],
        "settlement_block": a.get("settlement_block"),
        "overall": overall,
        "checks": checks,
        "total_percent_needed": round(total_pct, 6),
        "burn_weight": round(weights[burn_uid], 6),
        "rewarded_miners": len([r for r in rewards_by_uid.values() if r > 0]),
        "zeroed_miners": len([r for r in rewards_by_uid.values() if r == 0]),
        "gating_enabled": bool(cfg.get("REPUTATION_GATING_ENABLED")),
        "implied_tao_price": round(implied_price, 2) if implied_price else None,
        "archive_tao_price": chain.get("tao_price_usd"),
        "onchain_captured": (a.get("onchain") or {}).get("status") == "captured_live",
        "exported_at_unix": a.get("exported_at_unix"),
    }


def build_leaderboard(archives: dict[int, dict]) -> list[dict]:
    for epoch in sorted(archives, reverse=True):
        a = archives[epoch]
        inputs = a["inputs"]
        gated = inputs.get("gated") or {}
        rewards = {int(u): int(v) for u, v in (inputs.get("rewards_by_uid") or {}).items()}
        if not rewards:
            continue
        hk_by_uid = inputs.get("hotkeys_by_uid") or {}
        ref_scores = {int(u): float(w) for u, w in (a.get("reference", {}).get("scores") or {}).items()}
        rows = []
        for uid, reward in sorted(rewards.items(), key=lambda kv: -kv[1]):
            g = gated.get(str(uid), {})
            rows.append({
                "uid": uid,
                "hotkey": hk_by_uid.get(str(uid), g.get("hk_prefix", "?")),
                "volume": g.get("vol"),
                "reputation": g.get("rep"),
                "reward": reward,
                "weight_pct": round(ref_scores.get(uid, 0.0) * 100, 4),
                "status": "zeroed (rep gate)" if (g and reward == 0) else ("rewarded" if reward > 0 else "zeroed"),
            })
        return rows
    return []


# ---------------------------------------------------------------------------
# State manager
# ---------------------------------------------------------------------------
class AuditState:
    def __init__(self):
        self.lock = threading.Lock()
        self.state = {"status": "starting", "epochs": [], "leaderboard": [], "error": None}

    def refresh(self):
        try:
            archives = list_and_load_archives(ARCHIVE_SOURCE)
            epochs = []
            for e in sorted(archives, reverse=True):
                try:
                    epochs.append(audit_epoch(archives[e]))
                except Exception:
                    epochs.append({"epoch": e, "overall": FAIL,
                                   "checks": {"audit_error": check(FAIL, traceback.format_exc(limit=3))}})
            latest_cfg = archives[max(archives)]["config"] if archives else {}
            newest_export = max((a.get("exported_at_unix") or 0) for a in archives.values()) if archives else 0
            archive_age_s = (time.time() - newest_export) if newest_export else None
            stale = archive_age_s is None or archive_age_s > STALE_AFTER_S
            new_state = {
                "status": "stale" if stale else "ok",
                "stale": stale,
                "newest_archive_age_s": round(archive_age_s) if archive_age_s is not None else None,
                "stale_after_s": STALE_AFTER_S,
                "version": VERSION,
                "generated_at": time.time(),
                "archive_source": ARCHIVE_SOURCE,
                "archive_count": len(archives),
                "config_latest": latest_cfg,
                "epochs": epochs,
                "leaderboard": build_leaderboard(archives),
                "pass_count": sum(1 for e in epochs if e.get("overall") == PASS),
                "fail_count": sum(1 for e in epochs if e.get("overall") == FAIL),
                "error": None,
            }
        except Exception:
            new_state = {"status": "error", "error": traceback.format_exc(limit=5),
                         "generated_at": time.time(), "epochs": [], "leaderboard": []}
        with self.lock:
            self.state = new_state

    def get(self) -> dict:
        with self.lock:
            return self.state

    def run_forever(self):
        while True:
            self.refresh()
            time.sleep(RELOAD_INTERVAL_S)


STATE = AuditState()


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
DASHBOARD_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SN45 AlphaRidge Auditor</title>
<style>
:root { --bg:#f6f7f9; --card:#fff; --ink:#1a202c; --muted:#64748b; --line:#e2e8f0;
        --pass:#15803d; --pass-bg:#dcfce7; --fail:#b91c1c; --fail-bg:#fee2e2;
        --warn:#a16207; --warn-bg:#fef9c3; --skip:#475569; --skip-bg:#e2e8f0; }
@media (prefers-color-scheme: dark) {
  :root { --bg:#0f141a; --card:#161d26; --ink:#e5eaf0; --muted:#8fa0b3; --line:#26303c;
          --pass:#4ade80; --pass-bg:#14311f; --fail:#f87171; --fail-bg:#3b1414;
          --warn:#facc15; --warn-bg:#332b09; --skip:#94a3b8; --skip-bg:#242e39; } }
* { box-sizing:border-box; margin:0; }
body { font:14px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif; background:var(--bg); color:var(--ink); padding:24px; }
h1 { font-size:19px; margin-bottom:2px; } h2 { font-size:15px; margin:26px 0 10px; }
.sub { color:var(--muted); margin-bottom:20px; font-size:13px; }
.tiles { display:flex; flex-wrap:wrap; gap:12px; margin-bottom:8px; }
.tile { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:12px 18px; min-width:130px; }
.tile .v { font-size:22px; font-weight:650; } .tile .l { color:var(--muted); font-size:12px; }
.wrap { overflow-x:auto; background:var(--card); border:1px solid var(--line); border-radius:10px; }
table { border-collapse:collapse; width:100%; font-size:13px; }
th,td { text-align:left; padding:8px 12px; border-bottom:1px solid var(--line); white-space:nowrap; }
th { color:var(--muted); font-weight:600; font-size:12px; }
tr:last-child td { border-bottom:none; }
.badge { display:inline-block; padding:1px 8px; border-radius:999px; font-size:11.5px; font-weight:600; }
.PASS { color:var(--pass); background:var(--pass-bg); } .FAIL { color:var(--fail); background:var(--fail-bg); }
.WARN { color:var(--warn); background:var(--warn-bg); } .SKIP { color:var(--skip); background:var(--skip-bg); }
.hk { font-family:ui-monospace,monospace; font-size:12px; color:var(--muted); }
details { margin-top:4px; } summary { cursor:pointer; color:var(--muted); font-size:12px; }
.detail-line { padding:2px 0 2px 14px; font-size:12.5px; color:var(--muted); white-space:normal; }
#err { color:var(--fail); white-space:pre-wrap; }
#stale { display:none; background:var(--warn-bg); color:var(--warn); border:1px solid var(--warn);
         border-radius:10px; padding:10px 16px; margin-bottom:16px; font-weight:600; }
</style></head><body>
<h1>SN45 AlphaRidge Auditor</h1>
<div class="sub">Independent auditor: replays the subnet's scoring &amp; weight arithmetic from archived epoch data and verifies emissions are fair — no bittensor or LLM dependencies.</div>
<div class="sub" id="meta">loading…</div>
<div id="stale"></div>
<div class="tiles" id="tiles"></div>
<h2>Epoch audit history</h2><div class="wrap"><table id="epochs"></table></div>
<h2>Miner leaderboard (latest settled epoch)</h2><div class="wrap"><table id="board"></table></div>
<pre id="err"></pre>
<script>
const CHECKS = ["gate_replay","weight_replay","validator_reported","uint16_replay","onchain_match","dust_budget","store_consistency"];
const NAMES  = {gate_replay:"Gate", weight_replay:"Weights", validator_reported:"Vs logged", uint16_replay:"Uint16", onchain_match:"On-chain", dust_budget:"Dust/burn", store_consistency:"Store"};
function badge(s){ return `<span class="badge ${s}">${s}</span>`; }
async function refresh(){
  try {
    const s = await (await fetch("/api/state")).json();
    document.getElementById("err").textContent = s.error || "";
    const age = s.newest_archive_age_s;
    document.getElementById("meta").textContent =
      `source: ${s.archive_source} · ${s.archive_count||0} archives · newest archive ${age!=null?Math.round(age/60)+" min ago":"n/a"} · generated ${new Date((s.generated_at||0)*1000).toLocaleString()} · v${s.version||""}`;
    const staleEl = document.getElementById("stale");
    if (s.stale) {
      staleEl.style.display = "block";
      staleEl.textContent = `⚠ STALE DATA — newest archive is ${age!=null?Math.round(age/60)+" minutes":"unknown"} old (threshold ${Math.round((s.stale_after_s||0)/60)} min). The exporter, validator logging, or archive node has stalled; results below reflect the last data received, not the current chain state.`;
    } else { staleEl.style.display = "none"; }
    const cfg = s.config_latest||{};
    document.getElementById("tiles").innerHTML = [
      [s.epochs.length, "epochs audited"],
      [s.pass_count ?? "–", "pass"], [s.fail_count ?? "–", "fail"],
      [s.epochs[0] ? badge(s.epochs[0].overall) : "–", `latest epoch ${s.epochs[0] ? s.epochs[0].epoch : ""}`],
      [cfg.REPUTATION_GATING_ENABLED ? "ON" : "off", "reputation gating"],
      [s.epochs[0] ? (s.epochs[0].burn_weight*100).toFixed(2)+"%" : "–", "burn weight (latest)"],
    ].map(([v,l])=>`<div class="tile"><div class="v">${v}</div><div class="l">${l}</div></div>`).join("");
    document.getElementById("epochs").innerHTML =
      `<tr><th>epoch</th><th>overall</th>${CHECKS.map(c=>`<th>${NAMES[c]}</th>`).join("")}<th>total %</th><th>burn</th><th>miners</th><th>details</th></tr>` +
      s.epochs.map(e=>{
        const det = CHECKS.map(c=>e.checks[c]?`<div class="detail-line"><b>${NAMES[c]}</b>: ${e.checks[c].detail}</div>`:"").join("");
        return `<tr><td>${e.epoch}</td><td>${badge(e.overall)}</td>`+
          CHECKS.map(c=>`<td>${e.checks[c]?badge(e.checks[c].status):""}</td>`).join("")+
          `<td>${e.total_percent_needed??""}</td><td>${e.burn_weight??""}</td>`+
          `<td>${e.rewarded_miners??""}${e.zeroed_miners?` (+${e.zeroed_miners} zeroed)`:""}</td>`+
          `<td><details><summary>show</summary>${det}</details></td></tr>`;
      }).join("");
    document.getElementById("board").innerHTML =
      `<tr><th>#</th><th>uid</th><th>hotkey</th><th>volume</th><th>reputation</th><th>reward pts</th><th>weight %</th><th>status</th></tr>` +
      (s.leaderboard||[]).map((m,i)=>`<tr><td>${i+1}</td><td>${m.uid}</td><td class="hk">${m.hotkey||""}</td>`+
        `<td>${m.volume??""}</td><td>${m.reputation??""}</td><td>${m.reward}</td><td>${m.weight_pct}</td><td>${m.status}</td></tr>`).join("");
  } catch(e){ document.getElementById("err").textContent = String(e); }
}
refresh(); setInterval(refresh, 30000);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    timeout = 15  # per-handler socket timeout: stalled connections can't hold a thread forever
    server_version = "sn45audit/" + VERSION

    def _send(self, code: int, body: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/health":
            st = STATE.get()
            body = json.dumps({"status": st.get("status"), "epochs": len(st.get("epochs", [])),
                               "stale": st.get("stale"), "newest_archive_age_s": st.get("newest_archive_age_s"),
                               "generated_at": st.get("generated_at")}).encode()
            # stale data returns 503 so external monitors treat a stalled
            # exporter/validator-log pipeline as unhealthy, not just a dead server
            self._send(200 if st.get("status") == "ok" else 503, body, "application/json")
        elif path == "/api/state":
            self._send(200, json.dumps(STATE.get()).encode(), "application/json")
        elif path in ("/", "/index.html"):
            self._send(200, DASHBOARD_HTML.encode(), "text/html; charset=utf-8")
        else:
            self._send(404, b'{"error":"not found"}', "application/json")

    def log_message(self, fmt, *args):
        pass  # keep pm2 logs quiet; health checks would flood them


def main():
    STATE.refresh()
    t = threading.Thread(target=STATE.run_forever, daemon=True)
    t.start()
    server = ThreadingHTTPServer((BIND, PORT), Handler)
    server.daemon_threads = True
    print(f"[sidecar] serving on http://{BIND}:{PORT} (archives: {ARCHIVE_SOURCE})", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
