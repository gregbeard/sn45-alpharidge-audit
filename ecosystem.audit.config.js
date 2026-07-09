// sn45 audit sidecar — pm2 config
//
// All deployment-specific settings live in .env next to this file
// (cp .env.example .env and edit). Both Python processes load .env themselves;
// this file only needs it to locate the validator venv interpreter.
//
//   sn45-audit-exporter  needs bittensor (chain access) -> runs in the
//                        validator repo's venv; READ-ONLY against the validator
//                        (pm2 logs, state JSON files, archive subtensor node).
//   sn45-audit-sidecar   stdlib-only single file; replays the scoring
//                        arithmetic from the archives and serves the dashboard
//                        (default port 18889: /, /api/state, /health).
const fs = require("fs");
const path = require("path");

const env = {};
const envPath = path.join(__dirname, ".env");
if (fs.existsSync(envPath)) {
  for (const line of fs.readFileSync(envPath, "utf8").split("\n")) {
    const m = line.match(/^\s*([A-Za-z0-9_]+)\s*=\s*(.*?)\s*$/);
    if (m && !line.trim().startsWith("#")) env[m[1]] = m[2].replace(/^["']|["']$/g, "");
  }
}

const repo = env.ALPHARIDGE_REPO || "";
const interpreter = env.VALIDATOR_VENV_PYTHON || (repo ? path.join(repo, ".venv", "bin", "python") : "python3");

module.exports = {
  apps: [
    {
      name: "sn45-audit-exporter",
      script: "exporter/export_epoch_archives.py",
      interpreter,
      cwd: __dirname,
      max_memory_restart: "1G",
      autorestart: true,
      restart_delay: 15000,
    },
    {
      name: "sn45-audit-sidecar",
      script: "sidecar_audit.py",
      interpreter: env.SIDECAR_PYTHON || "python3",
      cwd: __dirname,
      max_memory_restart: "300M",
      autorestart: true,
      restart_delay: 5000,
    },
  ],
};
