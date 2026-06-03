#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
exec ../.venv/bin/python app.py \
  --host 0.0.0.0 \
  --port "${PORT:-8100}" \
  --ray-address "${RAY_ADDRESS:-auto}" \
  --dashboard-url "${DASHBOARD_URL:-}"
