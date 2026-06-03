#!/usr/bin/env bash
set -euo pipefail

HEAD_IP="${HEAD_IP:-172.31.34.33}"
PUBLIC_HEAD_IP="${PUBLIC_HEAD_IP:-}"
RAY_PORT="${RAY_PORT:-6379}"
DASHBOARD_PORT="${DASHBOARD_PORT:-8265}"
WEB_PORT="${WEB_PORT:-8080}"
GRID_PORT="${GRID_PORT:-8090}"
WORDCOUNT_PORT="${WORDCOUNT_PORT:-8100}"
UV="${UV:-/home/ubuntu/.local/bin/uv}"
UV_BIN_DIR="$(dirname "${UV}")"
DEMO_DIR="${DEMO_DIR:-/home/ubuntu/ds5110_s26_program_demo}"
export PATH="${UV_BIN_DIR}:${PATH}"
export RAY_ENABLE_UV_RUN_RUNTIME_ENV="${RAY_ENABLE_UV_RUN_RUNTIME_ENV:-0}"
unset VIRTUAL_ENV
WORKERS=(172.31.33.69 172.31.33.70)

cd "$(dirname "$0")"

fetch_public_ip() {
  local token
  token="$(curl -fsS --max-time 2 -X PUT \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" \
    "http://169.254.169.254/latest/api/token" 2>/dev/null || true)"

  if [ -n "${token}" ]; then
    curl -fsS --max-time 2 \
      -H "X-aws-ec2-metadata-token: ${token}" \
      "http://169.254.169.254/latest/meta-data/public-ipv4" 2>/dev/null || true
    return
  fi

  curl -fsS --max-time 2 \
    "http://169.254.169.254/latest/meta-data/public-ipv4" 2>/dev/null || true
}

if [ -z "${PUBLIC_HEAD_IP}" ]; then
  PUBLIC_HEAD_IP="$(fetch_public_ip)"
fi
if [ -z "${PUBLIC_HEAD_IP}" ]; then
  PUBLIC_HEAD_IP="${HEAD_IP}"
  echo "Could not resolve public IPv4 from instance metadata; using HEAD_IP=${HEAD_IP} for printed URLs."
fi

echo "Starting Ray head on ${HEAD_IP}:${RAY_PORT}..."
nohup "${UV}" run --active ray start \
  --head \
  --node-ip-address="${HEAD_IP}" \
  --port="${RAY_PORT}" \
  --dashboard-host=0.0.0.0 \
  --dashboard-port="${DASHBOARD_PORT}" \
  --num-cpus=2 \
  --disable-usage-stats \
  --block > ray-head.log 2>&1 &

echo "Waiting for Ray head to accept status requests..."
ray_ready=0
for _ in {1..120}; do
  if "${UV}" run --active ray status --address="${HEAD_IP}:${RAY_PORT}" >/tmp/ray-start-status.log 2>&1; then
    ray_ready=1
    echo "Ray head is ready."
    break
  fi
  sleep 1
done
if [ "${ray_ready}" -ne 1 ]; then
  echo "Ray head did not become ready within 120 seconds. See ${PWD}/ray-head.log"
  exit 1
fi

for worker in "${WORKERS[@]}"; do
  echo "Starting Ray worker on ${worker}..."
  ssh -f "ubuntu@${worker}" "unset VIRTUAL_ENV && export PATH='${UV_BIN_DIR}':\"\${PATH}\" && export RAY_ENABLE_UV_RUN_RUNTIME_ENV='${RAY_ENABLE_UV_RUN_RUNTIME_ENV}' && cd ${DEMO_DIR} && '${UV}' run --active ray start --address='${HEAD_IP}:${RAY_PORT}' --node-ip-address='${worker}' --num-cpus=2 --block > ray-worker.log 2>&1"
done

echo "Waiting for Ray workers to join..."
for _ in {1..60}; do
  node_count="$("${UV}" run --active python - <<'PY' 2>/dev/null || true
import ray
ray.init(address="auto")
print(sum(1 for node in ray.nodes() if node["Alive"]))
ray.shutdown()
PY
)"
  if [ "${node_count}" -ge 3 ]; then
    break
  fi
  sleep 1
done
echo "Ray live nodes: ${node_count:-0}"

echo "Ray dashboard: http://${PUBLIC_HEAD_IP}:${DASHBOARD_PORT}"
echo "Zombie game: http://${PUBLIC_HEAD_IP}:${WEB_PORT}"
echo "GridWorld game: http://${PUBLIC_HEAD_IP}:${GRID_PORT}"
echo "Word Count MapReduce: http://${PUBLIC_HEAD_IP}:${WORDCOUNT_PORT}"

echo "Starting Zombie game server..."
nohup "${UV}" run --active python zombie_game.py \
  --host 0.0.0.0 \
  --port "${WEB_PORT}" \
  --ray-address auto \
  --dashboard-url "http://${PUBLIC_HEAD_IP}:${DASHBOARD_PORT}" \
  > zombie-game.log 2>&1 &
zombie_pid=$!

(
  cd gridworld_escape
  echo "Starting GridWorld game server..."
  nohup "${UV}" run --active python app.py \
    --host 0.0.0.0 \
    --port "${GRID_PORT}" \
    --ray-address auto \
    > ../gridworld-game.log 2>&1 &
  echo $! > ../gridworld-game.pid
)
grid_pid="$(cat gridworld-game.pid)"
rm -f gridworld-game.pid

(
  cd word_count_mr
  echo "Starting Word Count MapReduce server..."
  nohup "${UV}" run --active python app.py \
    --host 0.0.0.0 \
    --port "${WORDCOUNT_PORT}" \
    --ray-address auto \
    --dashboard-url "http://${PUBLIC_HEAD_IP}:${DASHBOARD_PORT}" \
    > ../word-count-mr.log 2>&1 &
  echo $! > ../word-count-mr.pid
)
word_count_pid="$(cat word-count-mr.pid)"
rm -f word-count-mr.pid

echo "Zombie game PID: ${zombie_pid} log: ${PWD}/zombie-game.log"
echo "GridWorld game PID: ${grid_pid} log: ${PWD}/gridworld-game.log"
echo "Word Count MapReduce PID: ${word_count_pid} log: ${PWD}/word-count-mr.log"
echo "Startup complete."
