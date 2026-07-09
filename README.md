# sn45 audit sidecar

Standalone audit for the sn45 (alpharidge-ai) validator, satisfying the
gatekeeper requirement for a secondary process that verifies mining is fair and
auditable. **Nothing here modifies the validator repo or process** — all inputs
are read-only (pm2 logs, validator state JSON files, archive subtensor node).

## Components

| process | file | deps | role |
|---|---|---|---|
| `sn45-audit-exporter` | `exporter/export_epoch_archives.py` | bittensor (alpharidge-ai venv) | builds per-epoch archives in `data/epoch_archives/` |
| `sn45-audit-sidecar` | `sidecar_audit.py` | **stdlib only** | replays scoring arithmetic from archives; dashboard on **port 18889** |

Endpoints: `http://<host>:18889/` (dashboard) · `/api/state` (JSON) · `/health`
(`/health` returns HTTP 503 and the dashboard shows a STALE banner if no new
archive has appeared within `AUDIT_STALE_AFTER_S`, default 60 min.)

## Quickstart (any sn45 validator)

Prereqs: a running alpharidge-ai validator under pm2, its repo checkout on the
same box, python3, and a subtensor **archive** node (historical state queries).

```bash
git clone <this repo> sn45_sidecar_audit && cd sn45_sidecar_audit
cp .env.example .env
# edit .env — 3 required values:
#   ALPHARIDGE_REPO            path to your alpharidge-ai checkout
#   ARCHIVE_SUBTENSOR_ENDPOINT ws:// or wss:// archive node
#                              (public fallback: wss://archive.chain.opentensor.ai:443)
#   VALIDATOR_HOTKEY_SS58      your validator hotkey
# plus VALIDATOR_LOG_PREFIX if your pm2 process isn't named "alpharidge.validator"
pm2 start ecosystem.audit.config.js
curl localhost:18889/health
```

The exporter fails fast with a clear error if a required value is missing, so
it cannot silently audit the wrong validator. Archives accumulate from your
own validator's logs/state — history starts at deployment (the validator only
retains ~10 epochs of state, so there is nothing older to backfill).

## What gets verified per epoch

1. **gate_replay** — `reward == round(emission(rep) × volume)` per miner
   (reputation gating is currently served ON by the API; the sidecar also
   implements the non-gated `points > penalties` rule and picks per the
   archived flag)
2. **weight_replay** — the alpha-economics formula from `utils/burn.py`:
   `percent_i = max((r_i/alpha_per_point)/total_alpha·100, r_i·MIN_PERCENT_PER_POINT)`,
   scale-down if Σ>100, `weight = percent·scale/100`
3. **validator_reported** — replay vs the validator's own logged
   `total_percent_needed` / weights / burn weight (tolerance
   `AUDIT_PRICE_DRIFT_TOL`, default 5%, covers the TAO/USD price-cache drift —
   the one input with no historical source; the dashboard reports the implied
   price)
4. **uint16_replay** — L1-norm → max-weight clip → max-upscale → `round(w·65535)`
5. **onchain_match** — replayed fractions vs the weight vector actually
   observed on chain (captured live when `metagraph.last_update` advances;
   the chain only exposes the *latest* weights, so backfilled epochs skip this)
6. **dust_budget** — every unscored/zero-reward miner has exactly zero weight;
   the sole remainder sink is burn UID 189 and equals
   `1 − min(total_percent,100)/100` exactly. (The subnet has no per-miner
   epsilon/dust allocation — this check proves that invariant.)
7. **store_consistency** — validator log claims vs its on-disk `.reward_store.json`

## Key facts discovered while building (matter for interpreting results)

- The live validator runs **API-served config** that overrides repo defaults:
  `USD_PRICE_PER_POINT = 0.0006` (repo default 0.04), `MIN_PERCENT_PER_POINT =
  0.001`, `EMISSION_MIDPOINT = 0.57`, `REPUTATION_GATING_ENABLED = true`.
  The exporter captures served values from the validator's `[REMOTE_CONFIG]`
  log lines.
- `update_scores()` is a **direct overwrite**, not an EMA (docstring is stale).
- Burn is `weights[189] = 1 − min(total_percent,100)/100` in
  `utils/burn.py:calculate_weights` — the burn code inside `set_weights()` is
  commented out / dead.
- Validator state files only retain ~10 epochs (reward/penalty stores) and
  3 epochs (broadcast stores) — the exporter must keep running to accumulate
  history.

## Config (env)

Exporter: `ARCHIVE_SUBTENSOR_ENDPOINT` (ws://192.168.69.55:9944),
`VALIDATOR_HOTKEY_SS58`, `AUDIT_DATA_DIR`, `POLL_INTERVAL_SECONDS`,
`PM2_LOG_DIR`, `VALIDATOR_LOG_PREFIX`.

Sidecar: `AUDIT_PORT` (18889), `AUDIT_BIND`, `AUDIT_ARCHIVE_SOURCE` (local dir
**or** public https base URL serving the archives + `index.json`),
`AUDIT_RELOAD_INTERVAL_S`, `AUDIT_PRICE_DRIFT_TOL`, `AUDIT_MAX_EPOCHS`.
