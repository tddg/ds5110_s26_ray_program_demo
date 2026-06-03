import argparse
import json
import random
import socket
import threading
import time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


DEFAULT_ENV = {
    "width": 4,
    "height": 4,
    "start": [0, 0],
    "treasure": [3, 0],
    "exit": [3, 3],
    "lava": [[2, 2]],
    "walls": [[1, 1]],
}
ACTIONS = ("up", "down", "left", "right")
DELTAS = {
    "up": (0, -1),
    "down": (0, 1),
    "left": (-1, 0),
    "right": (1, 0),
}


def encode_state(x, y, has_treasure):
    return f"{x},{y},{1 if has_treasure else 0}"


def cell_tuple(cell):
    return int(cell[0]), int(cell[1])


def env_sets(env):
    return {
        "start": cell_tuple(env["start"]),
        "treasure": cell_tuple(env["treasure"]),
        "exit": cell_tuple(env["exit"]),
        "lava": {cell_tuple(cell) for cell in env["lava"]},
        "walls": {cell_tuple(cell) for cell in env["walls"]},
    }


def default_env():
    return json.loads(json.dumps(DEFAULT_ENV))


def normalize_env(raw_env):
    width = min(max(int(raw_env.get("width", 4)), 3), 50)
    height = min(max(int(raw_env.get("height", 4)), 3), 50)

    def clean_cell(value, fallback):
        try:
            x, y = int(value[0]), int(value[1])
        except (TypeError, ValueError, IndexError):
            x, y = fallback
        return [min(max(x, 0), width - 1), min(max(y, 0), height - 1)]

    for required in ("start", "treasure", "exit"):
        if raw_env.get(required) is None:
            raise ValueError(f"{required} must be placed before saving")

    env = {
        "width": width,
        "height": height,
        "start": clean_cell(raw_env.get("start"), (0, 0)),
        "treasure": clean_cell(raw_env.get("treasure"), (width - 1, 0)),
        "exit": clean_cell(raw_env.get("exit"), (width - 1, height - 1)),
        "lava": [],
        "walls": [],
    }
    if len({tuple(env["start"]), tuple(env["treasure"]), tuple(env["exit"])}) != 3:
        raise ValueError("start, treasure, and exit must be in different cells")

    reserved = {tuple(env["start"]), tuple(env["treasure"]), tuple(env["exit"])}
    seen = set()
    for name in ("lava", "walls"):
        for raw_cell in raw_env.get(name, []):
            cell = clean_cell(raw_cell, (0, 0))
            key = tuple(cell)
            if key in reserved or key in seen:
                continue
            env[name].append(cell)
            seen.add(key)
    return env


def empty_q(env):
    return {encode_state(x, y, has): {action: 0.0 for action in ACTIONS}
            for x in range(env["width"])
            for y in range(env["height"])
            for has in (False, True)
            if (x, y) not in env_sets(env)["walls"]}


def valid_move(env, x, y, action):
    sets = env_sets(env)
    dx, dy = DELTAS[action]
    nx, ny = x + dx, y + dy
    if nx < 0 or nx >= env["width"] or ny < 0 or ny >= env["height"] or (nx, ny) in sets["walls"]:
        return x, y
    return nx, ny


def step_env(env, state, action):
    sets = env_sets(env)
    x, y, has_treasure = state
    nx, ny = valid_move(env, x, y, action)
    reward = -1
    done = False

    if (nx, ny) == (x, y):
        return (nx, ny, has_treasure), reward, done, "wall"

    if (nx, ny) in sets["lava"]:
        start_x, start_y = sets["start"]
        return (start_x, start_y, False), -10, True, "lava"

    if (nx, ny) == sets["treasure"] and not has_treasure:
        has_treasure = True
        reward += 10

    if (nx, ny) == sets["exit"]:
        if has_treasure:
            return (nx, ny, has_treasure), reward + 20, True, "success"
        reward -= 4

    return (nx, ny, has_treasure), reward, done, "step"


def choose_action(q_table, state_key, epsilon, rng):
    if rng.random() < epsilon:
        return rng.choice(ACTIONS)
    values = q_table[state_key]
    best = max(values.values())
    choices = [action for action, value in values.items() if value == best]
    return rng.choice(choices)


def run_episode(env, q_table, epsilon, rng):
    sets = env_sets(env)
    state = (sets["start"][0], sets["start"][1], False)
    total_reward = 0
    for _step in range(env["width"] * env["height"] * 4):
        state_key = encode_state(*state)
        action = choose_action(q_table, state_key, epsilon, rng)
        next_state, reward, done, event = step_env(env, state, action)
        total_reward += reward
        state = next_state
        if done:
            return total_reward, event == "success"
    return total_reward, False


def evaluate_policy(env, q_table, episodes=100):
    rng = random.Random(12345)
    rewards = []
    successes = 0
    for _ in range(episodes):
        reward, success = run_episode(env, q_table, 0.0, rng)
        rewards.append(reward)
        successes += int(success)
    return {
        "episodes": episodes,
        "avg_reward": round(sum(rewards) / len(rewards), 2),
        "success_rate": round(successes / episodes, 3),
    }


def merge_q_tables(env, tables):
    merged = empty_q(env)
    counts = defaultdict(int)
    sums = defaultdict(float)
    for table in tables:
        for state_key, values in table.items():
            for action, value in values.items():
                key = (state_key, action)
                sums[key] += value
                counts[key] += 1
    for state_key, values in merged.items():
        for action in values:
            key = (state_key, action)
            if counts[key]:
                values[action] = sums[key] / counts[key]
    return merged


def greedy_policy_path(env, q_table, max_steps=None):
    sets = env_sets(env)
    max_steps = max_steps or min(40, env["width"] * env["height"] * 2)
    state = (sets["start"][0], sets["start"][1], False)
    path = [{"x": state[0], "y": state[1], "has_treasure": state[2], "event": "start"}]
    seen = set()
    for _ in range(max_steps):
        state_key = encode_state(*state)
        action = max(q_table[state_key], key=q_table[state_key].get)
        state, reward, done, event = step_env(env, state, action)
        loop_key = (state[0], state[1], state[2], action)
        path.append({
            "x": state[0],
            "y": state[1],
            "has_treasure": state[2],
            "action": action,
            "reward": reward,
            "event": "loop" if loop_key in seen and not done else event,
        })
        if done or loop_key in seen:
            break
        seen.add(loop_key)
    return path


@ray.remote
def train_worker(env, base_q, episodes, alpha, gamma, epsilon, seed):
    sets = env_sets(env)
    rng = random.Random(seed)
    q_table = json.loads(json.dumps(base_q))
    rewards = []
    successes = 0

    for _ in range(episodes):
        state = (sets["start"][0], sets["start"][1], False)
        total_reward = 0
        for _step in range(env["width"] * env["height"] * 4):
            state_key = encode_state(*state)
            action = choose_action(q_table, state_key, epsilon, rng)
            next_state, reward, done, event = step_env(env, state, action)
            next_key = encode_state(*next_state)
            best_next = max(q_table[next_key].values())
            current = q_table[state_key][action]
            q_table[state_key][action] = current + alpha * (reward + gamma * best_next - current)
            total_reward += reward
            state = next_state
            if done:
                if event == "success":
                    successes += 1
                break
        rewards.append(total_reward)

    return {
        "q_table": q_table,
        "episodes": episodes,
        "avg_reward": sum(rewards) / len(rewards),
        "success_rate": successes / episodes,
        "node_ip": ray.util.get_node_ip_address(),
        "hostname": socket.gethostname(),
    }


class Trainer:
    def __init__(self, workers, episodes_per_worker, alpha, gamma, epsilon):
        self.workers = workers
        self.episodes_per_worker = episodes_per_worker
        self.alpha = alpha
        self.gamma = gamma
        self.epsilon = epsilon
        self.env = default_env()
        self.q_table = empty_q(self.env)
        self.lock = threading.Lock()
        self.training = False
        self.env_version = 0
        self.total_episodes = 0
        self.round = 0
        self.history = []
        self.last_workers = []
        self.training_started_at = None
        self.greedy_100_at = None
        self.last_evaluation = evaluate_policy(self.env, self.q_table)

    def start(self):
        with self.lock:
            if self.training:
                return
            self.training_started_at = time.time()
            self.greedy_100_at = None
            self.training = True
        threading.Thread(target=self._loop, daemon=True).start()

    def stop(self):
        with self.lock:
            self.training = False

    def reset(self):
        with self.lock:
            self.q_table = empty_q(self.env)
            self.env_version += 1
            self.total_episodes = 0
            self.round = 0
            self.history = []
            self.last_workers = []
            self.training_started_at = None
            self.greedy_100_at = None
            self.last_evaluation = evaluate_policy(self.env, self.q_table)

    def update_env(self, env):
        normalized = normalize_env(env)
        with self.lock:
            self.training = False
            self.env = normalized
            self.q_table = empty_q(self.env)
            self.env_version += 1
            self.total_episodes = 0
            self.round = 0
            self.history = []
            self.last_workers = []
            self.training_started_at = None
            self.greedy_100_at = None
            self.last_evaluation = evaluate_policy(self.env, self.q_table)
            return self.env

    def configure(self, alpha=None, epsilon=None):
        with self.lock:
            if alpha is not None:
                self.alpha = float(alpha)
            if epsilon is not None:
                self.epsilon = float(epsilon)

    def _loop(self):
        while True:
            with self.lock:
                if not self.training:
                    return
                env = json.loads(json.dumps(self.env))
                env_version = self.env_version
                base_q = json.loads(json.dumps(self.q_table))
                alpha = self.alpha
                gamma = self.gamma
                epsilon = self.epsilon
                round_id = self.round + 1

            live_nodes = [node for node in ray.nodes() if node["Alive"]]
            live_nodes.sort(key=lambda node: node["NodeManagerAddress"])
            node_ids = [node["NodeID"] for node in live_nodes]
            refs = []
            for worker_index in range(self.workers):
                options = {"num_cpus": 0.5}
                if node_ids:
                    options["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                        node_id=node_ids[worker_index % len(node_ids)],
                        soft=False,
                    )
                refs.append(
                    train_worker.options(**options).remote(
                    env,
                    base_q,
                    self.episodes_per_worker,
                    alpha,
                    gamma,
                    epsilon,
                    int(time.time() * 1000) + worker_index + round_id * 100,
                    )
                )
            results = ray.get(refs)
            merged = merge_q_tables(env, [result["q_table"] for result in results])
            avg_reward = sum(result["avg_reward"] for result in results) / len(results)
            success_rate = sum(result["success_rate"] for result in results) / len(results)
            evaluation = evaluate_policy(env, merged)

            with self.lock:
                if env_version != self.env_version:
                    continue
                self.q_table = merged
                self.last_evaluation = evaluation
                if (
                    self.greedy_100_at is None
                    and self.training_started_at is not None
                    and evaluation["success_rate"] >= 1.0
                ):
                    self.greedy_100_at = time.time()
                self.round = round_id
                self.total_episodes += self.workers * self.episodes_per_worker
                self.last_workers = [
                    {
                        "node_ip": result["node_ip"],
                        "hostname": result["hostname"],
                        "episodes": result["episodes"],
                        "avg_reward": round(result["avg_reward"], 2),
                        "success_rate": round(result["success_rate"], 3),
                    }
                    for result in results
                ]
                self.history.append({
                    "round": self.round,
                    "episodes": self.total_episodes,
                    "avg_reward": round(avg_reward, 2),
                    "success_rate": round(success_rate, 3),
                })
                self.history = self.history[-80:]

    def snapshot(self):
        with self.lock:
            env = json.loads(json.dumps(self.env))
            policy = {
                state: max(values, key=values.get)
                for state, values in self.q_table.items()
            }
            elapsed_to_100 = None
            if self.greedy_100_at is not None and self.training_started_at is not None:
                elapsed_to_100 = round(self.greedy_100_at - self.training_started_at, 2)
            return {
                "env": env,
                "training": self.training,
                "round": self.round,
                "episodes": self.total_episodes,
                "alpha": self.alpha,
                "gamma": self.gamma,
                "epsilon": self.epsilon,
                "history": self.history,
                "evaluation": self.last_evaluation,
                "time_to_greedy_100": elapsed_to_100,
                "workers": self.last_workers,
                "policy": policy,
                "path": greedy_policy_path(env, self.q_table),
                "nodes": [
                    {
                        "ip": node["NodeManagerAddress"],
                        "alive": node["Alive"],
                        "cpu": node.get("Resources", {}).get("CPU", 0),
                    }
                    for node in ray.nodes()
                ],
            }


def load_index():
    return Path(__file__).with_name("web").joinpath("index.html").read_bytes()


def make_handler(trainer):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            return

        def json_response(self, payload, status=200):
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path == "/":
                body = load_index()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/state":
                self.json_response(trainer.snapshot())
                return
            if parsed.path == "/train":
                trainer.start()
                self.json_response({"ok": True, "training": True})
                return
            if parsed.path == "/stop":
                trainer.stop()
                self.json_response({"ok": True, "training": False})
                return
            if parsed.path == "/reset":
                trainer.stop()
                trainer.reset()
                self.json_response({"ok": True})
                return
            if parsed.path == "/config":
                query = parse_qs(parsed.query)
                trainer.configure(
                    alpha=query.get("alpha", [None])[0],
                    epsilon=query.get("epsilon", [None])[0],
                )
                self.json_response({"ok": True})
                return
            self.send_response(404)
            self.end_headers()

        def do_POST(self):
            parsed = urlparse(self.path)
            if parsed.path == "/env":
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                try:
                    env = json.loads(body.decode("utf-8"))
                    normalized = trainer.update_env(env)
                except (json.JSONDecodeError, TypeError, ValueError) as exc:
                    self.json_response({"ok": False, "error": str(exc)}, status=400)
                    return
                self.json_response({"ok": True, "env": normalized})
                return
            self.send_response(404)
            self.end_headers()

    return Handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--ray-address", default="auto")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--episodes-per-worker", type=int, default=500)
    args = parser.parse_args()

    ray.init(address=args.ray_address)
    trainer = Trainer(
        workers=args.workers,
        episodes_per_worker=args.episodes_per_worker,
        alpha=0.25,
        gamma=0.92,
        epsilon=0.25,
    )
    server = ThreadingHTTPServer((args.host, args.port), make_handler(trainer))
    print(f"GridWorld Treasure Escape: http://{socket.gethostbyname(socket.gethostname())}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
