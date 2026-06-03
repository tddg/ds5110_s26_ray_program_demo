import argparse
import json
import math
import os
import random
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


GRID_WIDTH = 80
GRID_HEIGHT = 44
REGION_COLS = 2
REGION_ROWS = 2
HUMANS_PER_REGION = 70
ZOMBIES_PER_REGION = 2
INFECTION_RADIUS = 1.45
INFECTION_PROBABILITY = 0.65
HUMAN_STEP = 1.3
ZOMBIE_STEP = 0.95
TICK_SECONDS = 0.35
MIN_TASK_CPUS = 0.1
MAX_TASK_CPUS = 1.0
MAX_CPU_BURN_SECONDS = 0.45


def region_bounds(region_id):
    col = region_id % REGION_COLS
    row = region_id // REGION_COLS
    width = GRID_WIDTH // REGION_COLS
    height = GRID_HEIGHT // REGION_ROWS
    x0 = col * width
    y0 = row * height
    x1 = GRID_WIDTH if col == REGION_COLS - 1 else x0 + width
    y1 = GRID_HEIGHT if row == REGION_ROWS - 1 else y0 + height
    return x0, y0, x1, y1


def make_region(region_id):
    x0, y0, x1, y1 = region_bounds(region_id)
    rng = random.Random(10_000 + region_id)
    agents = []

    for idx in range(HUMANS_PER_REGION):
        agents.append(
            {
                "id": f"{region_id}-h-{idx}",
                "kind": "human",
                "x": rng.uniform(x0 + 1, x1 - 1),
                "y": rng.uniform(y0 + 1, y1 - 1),
            }
        )

    for idx in range(ZOMBIES_PER_REGION):
        agents.append(
            {
                "id": f"{region_id}-z-{idx}",
                "kind": "zombie",
                "x": rng.uniform(x0 + 1, x1 - 1),
                "y": rng.uniform(y0 + 1, y1 - 1),
            }
        )

    return {
        "region_id": region_id,
        "bounds": [x0, y0, x1, y1],
        "agents": agents,
        "rng_seed": 42_000 + region_id,
    }


def burn_cpu(seconds):
    if seconds <= 0:
        return
    deadline = time.perf_counter() + seconds
    value = 0.12345
    while time.perf_counter() < deadline:
        for index in range(900):
            value = math.sin(value + index) * math.cos(value - index)


@ray.remote
def simulate_region_step(region, tick, cpu_load):
    """One Ray task simulates one region for one timestep."""
    rng = random.Random(region["rng_seed"] + tick * 7919)
    x0, y0, x1, y1 = region["bounds"]
    agents = region["agents"]
    zombies = [agent for agent in agents if agent["kind"] == "zombie"]

    for agent in agents:
        speed = ZOMBIE_STEP if agent["kind"] == "zombie" else HUMAN_STEP
        angle = rng.random() * math.tau
        agent["x"] = min(max(agent["x"] + math.cos(angle) * speed, x0 + 0.4), x1 - 0.4)
        agent["y"] = min(max(agent["y"] + math.sin(angle) * speed, y0 + 0.4), y1 - 0.4)

    for human in agents:
        if human["kind"] != "human":
            continue
        for zombie in zombies:
            dx = human["x"] - zombie["x"]
            dy = human["y"] - zombie["y"]
            if dx * dx + dy * dy <= INFECTION_RADIUS * INFECTION_RADIUS:
                if rng.random() < INFECTION_PROBABILITY:
                    human["kind"] = "zombie"
                    zombies.append(human)
                break

    burn_cpu(MAX_CPU_BURN_SECONDS * cpu_load)

    humans = sum(1 for agent in agents if agent["kind"] == "human")
    zombie_count = len(agents) - humans
    return {
        "region": region,
        "stats": {
            "region_id": region["region_id"],
            "humans": humans,
            "zombies": zombie_count,
            "node_ip": ray.util.get_node_ip_address(),
            "hostname": socket.gethostname(),
        },
    }


@ray.remote
class GlobalStats:
    def __init__(self):
        self.tick = 0
        self.totals = {"humans": 0, "zombies": 0}
        self.regions = []
        self.agents = []
        self.node_map = {}
        self.history = []
        self.cpu_load = 0.0
        self.updated_at = time.time()

    def record(self, tick, region_stats, agents, node_map, cpu_load):
        humans = sum(region["humans"] for region in region_stats)
        zombies = sum(region["zombies"] for region in region_stats)
        self.tick = tick
        self.totals = {"humans": humans, "zombies": zombies}
        self.regions = region_stats
        self.agents = agents
        self.node_map = node_map
        self.cpu_load = cpu_load
        self.updated_at = time.time()
        self.history.append({"tick": tick, "humans": humans, "zombies": zombies})
        self.history = self.history[-160:]

    def snapshot(self):
        return {
            "tick": self.tick,
            "grid": {"width": GRID_WIDTH, "height": GRID_HEIGHT},
            "totals": self.totals,
            "regions": self.regions,
            "agents": self.agents,
            "node_map": self.node_map,
            "history": self.history,
            "cpu_load": self.cpu_load,
            "updated_at": self.updated_at,
        }


class Simulation:
    def __init__(self, stats_actor):
        self.stats_actor = stats_actor
        self.regions = [make_region(region_id) for region_id in range(REGION_COLS * REGION_ROWS)]
        self.tick = 0
        self.running = False
        self.thread = None
        self.lock = threading.Lock()
        self.node_ids = []
        self.node_map = {}
        self.cpu_load = 0.0

    def refresh_nodes(self):
        live_nodes = [node for node in ray.nodes() if node["Alive"]]
        live_nodes.sort(key=lambda node: node["NodeManagerAddress"])
        self.node_ids = [node["NodeID"] for node in live_nodes]
        self.node_map = {
            node["NodeID"]: {
                "ip": node["NodeManagerAddress"],
                "hostname": node.get("NodeName", node["NodeManagerAddress"]),
                "resources": node.get("Resources", {}),
            }
            for node in live_nodes
        }

    def start(self):
        with self.lock:
            if self.running:
                return
            self.running = True
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()

    def stop(self):
        with self.lock:
            self.running = False

    def reset(self):
        self.stop()
        self.regions = [make_region(region_id) for region_id in range(REGION_COLS * REGION_ROWS)]
        self.tick = 0
        self.start()

    def set_cpu_load(self, value):
        with self.lock:
            self.cpu_load = min(max(float(value), 0.0), 1.0)
            return self.cpu_load

    def _run(self):
        while True:
            with self.lock:
                if not self.running:
                    return
            self.refresh_nodes()
            with self.lock:
                cpu_load = self.cpu_load
            task_cpus = MIN_TASK_CPUS + (MAX_TASK_CPUS - MIN_TASK_CPUS) * cpu_load
            task_refs = []
            for index, region in enumerate(self.regions):
                if self.node_ids:
                    node_id = self.node_ids[index % len(self.node_ids)]
                    strategy = NodeAffinitySchedulingStrategy(node_id=node_id, soft=False)
                    ref = simulate_region_step.options(
                        scheduling_strategy=strategy,
                        num_cpus=task_cpus,
                    ).remote(region, self.tick + 1, cpu_load)
                else:
                    ref = simulate_region_step.options(num_cpus=task_cpus).remote(region, self.tick + 1, cpu_load)
                task_refs.append(ref)

            results = ray.get(task_refs)
            self.regions = [result["region"] for result in results]
            region_stats = [result["stats"] for result in results]
            agents = [
                {
                    "id": agent["id"],
                    "kind": agent["kind"],
                    "x": round(agent["x"], 2),
                    "y": round(agent["y"], 2),
                    "region_id": region["region_id"],
                }
                for region in self.regions
                for agent in region["agents"]
            ]
            self.tick += 1
            self.stats_actor.record.remote(self.tick, region_stats, agents, self.node_map, cpu_load)
            time.sleep(TICK_SECONDS)


def load_index():
    return Path(__file__).with_name("web").joinpath("index.html").read_bytes()


def make_handler(stats_actor, simulation, dashboard_url):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            return

        def _send_json(self, payload, status=200):
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/":
                body = load_index()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if path == "/state":
                snapshot = ray.get(stats_actor.snapshot.remote())
                snapshot["dashboard_url"] = dashboard_url
                self._send_json(snapshot)
                return
            if path == "/reset":
                simulation.reset()
                self._send_json({"ok": True})
                return
            if path == "/cpu":
                query = urlparse(self.path).query
                value = 0.0
                for part in query.split("&"):
                    key, _, raw = part.partition("=")
                    if key == "value":
                        value = raw
                        break
                self._send_json({"cpu_load": simulation.set_cpu_load(value)})
                return
            self.send_response(404)
            self.end_headers()

    return Handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--ray-address", default="auto")
    parser.add_argument("--dashboard-url", default="")
    args = parser.parse_args()

    ray.init(address=args.ray_address)
    stats_actor = GlobalStats.options(name="zombie-global-stats").remote()
    simulation = Simulation(stats_actor)
    simulation.start()

    server = ThreadingHTTPServer((args.host, args.port), make_handler(stats_actor, simulation, args.dashboard_url))
    print(f"Zombie Infection web app: http://{socket.gethostbyname(socket.gethostname())}:{args.port}")
    print(f"Ray dashboard: {args.dashboard_url}")
    server.serve_forever()


if __name__ == "__main__":
    main()
