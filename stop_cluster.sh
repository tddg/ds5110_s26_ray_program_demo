#!/usr/bin/env bash
set -euo pipefail

WORKERS=(172.31.33.69 172.31.33.70)
UV="${UV:-/home/ubuntu/.local/bin/uv}"
UV_BIN_DIR="$(dirname "${UV}")"
DEMO_DIR="${DEMO_DIR:-/home/ubuntu/ds5110_s26_program_demo}"
export PATH="${UV_BIN_DIR}:${PATH}"

cd "$(dirname "$0")"

stop_matching_processes() {
  local pattern="$1"
  local pids
  pids="$(pgrep -f "$pattern" || true)"
  if [ -n "$pids" ]; then
    kill $pids >/dev/null 2>&1 || true
    sleep 0.5
  fi
}

stop_matching_processes "zombie_game.py"
stop_matching_processes "gridworld_escape/app.py"
stop_matching_processes "word_count_mr/app.py"
stop_matching_processes "python app.py --host 0.0.0.0 --port 8090"
stop_matching_processes "python app.py --host 0.0.0.0 --port 8100"

for worker in "${WORKERS[@]}"; do
  echo "Stopping Ray runtime on ${worker}..."
  ssh "ubuntu@${worker}" "export PATH='${UV_BIN_DIR}':\"\${PATH}\" && cd ${DEMO_DIR} && '${UV}' run --active ray stop --force > ray-stop.log 2>&1 || true"
done

echo "Stopping local Ray runtime..."
"${UV}" run --active ray stop --force > ray-stop.log 2>&1 || true
