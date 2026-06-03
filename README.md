# DS5110 Spring 2026 Ray Program Demo

This repository contains small Ray programs for demonstrating distributed Python execution across a Ray cluster.

## Contents

- `ray_task_actor_demo.ipynb`: notebook introduction to Ray tasks and actors.
- `zombie_game.py` and `web/index.html`: live region simulation that schedules per-region Ray tasks across cluster nodes.
- `gridworld_escape/`: distributed Q-learning demo with a browser UI.
- `word_count_mr/`: MapReduce word-count demo with map and reduce Ray tasks, a web UI, and a small correctness test.
- `start_cluster.sh` and `stop_cluster.sh`: helper scripts for starting and stopping the Ray head, worker processes, and demo web apps on an EC2-style cluster.

Generated runtime files are intentionally not included. Logs, `.venv`, Python bytecode, notebook checkpoints, and process IDs are ignored by git.

## Setup

The project uses Python 3.12 and `uv`.

```bash
cd ~/ds5110_s26_program_demo
uv sync
```

If `uv` is installed somewhere other than `/home/ubuntu/.local/bin/uv`, set `UV` when running the scripts.

## Cluster Configuration

The cluster helper scripts default to the IP addresses used by the demo environment:

- Ray head: `172.31.34.33`
- Workers: `172.31.33.69`, `172.31.33.70`

Override these values as needed:

```bash
HEAD_IP=<head-private-ip> PUBLIC_HEAD_IP=<head-public-ip> ./start_cluster.sh
```

Worker nodes should have this repository available at the same path as the head node. Override the worker-side path with `DEMO_DIR` if needed:

```bash
DEMO_DIR=/home/ubuntu/ds5110_s26_program_demo ./start_cluster.sh
```

## Run All Demos

From the head node:

```bash
./start_cluster.sh
```

The script starts the Ray head, starts Ray workers over SSH, waits for the cluster to become ready, and launches the three web demos.

Default ports:

- Ray dashboard: `8265`
- Zombie game: `8080`
- GridWorld escape: `8090`
- Word Count MapReduce: `8100`

Stop the demos and Ray runtime:

```bash
./stop_cluster.sh
```

## Run Individual Demos

Start or connect to a Ray cluster first, then run an individual app:

```bash
uv run python zombie_game.py --host 0.0.0.0 --port 8080 --ray-address auto
```

```bash
cd gridworld_escape
PORT=8090 RAY_ADDRESS=auto ./start.sh
```

```bash
cd word_count_mr
PORT=8100 RAY_ADDRESS=auto ./start.sh
```

## GridWorld Q-Learning Design

The GridWorld demo trains an agent to collect treasure and reach the exit while avoiding lava and walls. A state is encoded as `x,y,has_treasure`, so the policy can learn different actions before and after treasure is collected. The Q-table maps each state to four action values: `up`, `down`, `left`, and `right`.

Training uses epsilon-greedy Q-learning. Each episode starts at the start cell, explores or follows the current best action, receives rewards from the environment, and updates the selected action value with:

```text
Q(s,a) = Q(s,a) + alpha * (reward + gamma * max(Q(next_state,*)) - Q(s,a))
```

The reward model gives a small cost for each move, a penalty for lava, a bonus for treasure, and the largest bonus for reaching the exit after collecting treasure.

The training is distributed with Ray. The HTTP server owns the current global Q-table, then each training round launches several `train_worker` Ray tasks. Every worker receives a copy of the environment and Q-table, runs many episodes independently, and returns an updated Q-table plus worker statistics. The server averages the returned Q-values to form the next global Q-table, evaluates the greedy policy, and exposes the latest policy, path, worker locations, and metrics through `/state` for the browser UI.

## Test

The included test exercises the MapReduce functions against a running Ray cluster:

```bash
cd word_count_mr
./test_mr.sh
```
