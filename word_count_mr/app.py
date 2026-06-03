import argparse
import hashlib
import json
import re
import socket
import threading
import time
from collections import Counter, defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


DEFAULT_TEXT = """Ray makes distributed Python feel direct.
Ray tasks run map functions in parallel.
MapReduce groups words by key and reduce tasks count every word.
Word count is a small demo, but Ray shows where each word travels."""
DEFAULT_MAP_TASKS = 4
DEFAULT_REDUCE_TASKS = 3
WORD_RE = re.compile(r"[A-Za-z0-9']+")


def map_function(map_input):
    """Convert input text into lowercase (word, 1) key-value pairs."""
    return [(match.group(0).lower(), 1) for match in WORD_RE.finditer(map_input)]


def reduce_function(vals):
    """Count the number of values associated with a word."""
    return len(vals)


def reduce_index_for_word(word, num_reduce_tasks):
    digest = hashlib.md5(word.encode("utf-8")).hexdigest()
    return int(digest, 16) % num_reduce_tasks


@ray.remote
def do_map_task(map_input, num_reduce_tasks):
    intermediate_results = [[] for _ in range(num_reduce_tasks)]
    for kv_pair in map_function(map_input):
        word, _count = kv_pair
        reduce_index = reduce_index_for_word(word, num_reduce_tasks)
        intermediate_results[reduce_index].append(kv_pair)
    node_ip = ray.util.get_node_ip_address()
    hostname = socket.gethostname()
    return tuple(
        {"pairs": bucket, "node_ip": node_ip, "hostname": hostname}
        for bucket in intermediate_results
    )


@ray.remote
def do_reduce_task(input_buckets):
    table = defaultdict(list)
    for bucket in input_buckets:
        pairs = bucket.get("pairs", bucket)
        for word, count in pairs:
            table[word].append(count)

    output = {}
    for word, vals in table.items():
        output[word] = reduce_function(vals)
    return {
        "output": output,
        "node_ip": ray.util.get_node_ip_address(),
        "hostname": socket.gethostname(),
    }


def split_text(text, num_map_tasks):
    words = WORD_RE.findall(text)
    if not words:
        return ["" for _ in range(num_map_tasks)]

    buckets = []
    chunk_size = max(1, (len(words) + num_map_tasks - 1) // num_map_tasks)
    for index in range(num_map_tasks):
        chunk = words[index * chunk_size:(index + 1) * chunk_size]
        buckets.append(" ".join(chunk))
    return buckets


def top_words(results, limit=20):
    merged = Counter()
    for partial in results:
        merged.update(reduce_counts(partial))
    return [{"word": word, "count": count} for word, count in merged.most_common(limit)]


def bucket_pairs(bucket):
    return bucket.get("pairs", bucket)


def reduce_counts(result):
    return result.get("output", result)


def node_snapshot():
    nodes = []
    for node in ray.nodes():
        nodes.append({
            "ip": node["NodeManagerAddress"],
            "alive": node["Alive"],
            "cpu": node.get("Resources", {}).get("CPU", 0),
        })
    return nodes


class WordCountRunner:
    def __init__(self, default_text, dashboard_url):
        self.default_text = default_text
        self.dashboard_url = dashboard_url
        self.lock = threading.Lock()
        self.running = False
        self.last_run = None
        self.error = None

    def start(self, text, num_map_tasks, num_reduce_tasks):
        text = text if text.strip() else self.default_text
        num_map_tasks = min(max(int(num_map_tasks), 1), 12)
        num_reduce_tasks = min(max(int(num_reduce_tasks), 1), 12)
        with self.lock:
            if self.running:
                return False
            self.running = True
            self.error = None
            self.last_run = {
                "status": "running",
                "started_at": time.time(),
                "text": text,
                "num_map_tasks": num_map_tasks,
                "num_reduce_tasks": num_reduce_tasks,
                "nodes": node_snapshot(),
            }
        thread = threading.Thread(
            target=self._run_pipeline,
            args=(text, num_map_tasks, num_reduce_tasks),
            daemon=True,
        )
        thread.start()
        return True

    def _run_pipeline(self, text, num_map_tasks, num_reduce_tasks):
        started_at = time.time()
        try:
            map_inputs = split_text(text, num_map_tasks)
            live_nodes = [node for node in ray.nodes() if node["Alive"]]
            live_nodes.sort(key=lambda node: node["NodeManagerAddress"])
            node_ids = [node["NodeID"] for node in live_nodes]
            map_refs = []
            for map_id, map_input in enumerate(map_inputs):
                options = {"num_returns": num_reduce_tasks, "num_cpus": 0.25}
                if node_ids:
                    options["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                        node_id=node_ids[map_id % len(node_ids)],
                        soft=False,
                    )
                refs = do_map_task.options(**options).remote(
                    map_input,
                    num_reduce_tasks,
                )
                map_refs.append(list(refs))

            map_outputs = [ray.get(refs) for refs in map_refs]
            reduce_inputs = [
                [map_outputs[map_id][reduce_id] for map_id in range(num_map_tasks)]
                for reduce_id in range(num_reduce_tasks)
            ]
            reduce_refs = []
            for reduce_id in range(num_reduce_tasks):
                options = {"num_cpus": 0.25}
                if node_ids:
                    options["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                        node_id=node_ids[(reduce_id + num_map_tasks) % len(node_ids)],
                        soft=False,
                    )
                reduce_refs.append(do_reduce_task.options(**options).remote(reduce_inputs[reduce_id]))
            reduce_outputs = ray.get(reduce_refs)

            trace = self._build_trace(map_inputs, map_outputs, reduce_inputs, reduce_outputs)
            all_counts = Counter()
            for partial in reduce_outputs:
                all_counts.update(reduce_counts(partial))

            payload = {
                "status": "complete",
                "started_at": started_at,
                "finished_at": time.time(),
                "duration_ms": round((time.time() - started_at) * 1000, 1),
                "text": text,
                "num_map_tasks": num_map_tasks,
                "num_reduce_tasks": num_reduce_tasks,
                "nodes": node_snapshot(),
                "map_tasks": trace["map_tasks"],
                "reduce_tasks": trace["reduce_tasks"],
                "top_words": top_words(reduce_outputs),
                "all_counts": dict(sorted(all_counts.items())),
            }
            with self.lock:
                self.last_run = payload
        except Exception as exc:
            with self.lock:
                self.error = str(exc)
                self.last_run = {
                    "status": "error",
                    "error": str(exc),
                    "finished_at": time.time(),
                    "text": text,
                    "num_map_tasks": num_map_tasks,
                    "num_reduce_tasks": num_reduce_tasks,
                    "nodes": node_snapshot(),
                }
        finally:
            with self.lock:
                self.running = False

    def _build_trace(self, map_inputs, map_outputs, reduce_inputs, reduce_outputs):
        map_tasks = []
        for map_id, map_input in enumerate(map_inputs):
            assignments = []
            for reduce_id, bucket in enumerate(map_outputs[map_id]):
                for word, count in bucket_pairs(bucket):
                    assignments.append({
                        "word": word,
                        "count": count,
                        "reduce_id": reduce_id,
                    })
            first_bucket = map_outputs[map_id][0] if map_outputs[map_id] else {}
            map_tasks.append({
                "id": map_id,
                "input": map_input,
                "node_ip": first_bucket.get("node_ip", ""),
                "hostname": first_bucket.get("hostname", ""),
                "emitted": map_function(map_input),
                "assignments": assignments,
                "buckets": [
                    {
                        "reduce_id": reduce_id,
                        "pairs": bucket_pairs(bucket),
                        "node_ip": bucket.get("node_ip", ""),
                        "hostname": bucket.get("hostname", ""),
                    }
                    for reduce_id, bucket in enumerate(map_outputs[map_id])
                ],
            })

        reduce_tasks = []
        for reduce_id, buckets in enumerate(reduce_inputs):
            grouped = defaultdict(list)
            for map_id, bucket in enumerate(buckets):
                for word, count in bucket_pairs(bucket):
                    grouped[word].append({"map_id": map_id, "count": count})
            reduce_result = reduce_outputs[reduce_id]
            reduce_tasks.append({
                "id": reduce_id,
                "node_ip": reduce_result.get("node_ip", ""),
                "hostname": reduce_result.get("hostname", ""),
                "inputs": [
                    {
                        "map_id": map_id,
                        "pairs": bucket_pairs(bucket),
                        "node_ip": bucket.get("node_ip", ""),
                        "hostname": bucket.get("hostname", ""),
                    }
                    for map_id, bucket in enumerate(buckets)
                ],
                "grouped": dict(sorted(grouped.items())),
                "output": dict(sorted(reduce_counts(reduce_result).items())),
            })
        return {"map_tasks": map_tasks, "reduce_tasks": reduce_tasks}

    def snapshot(self):
        with self.lock:
            return {
                "running": self.running,
                "error": self.error,
                "dashboard_url": self.dashboard_url,
                "default_text": self.default_text,
                "last_run": self.last_run,
                "nodes": node_snapshot(),
            }


def load_index():
    return Path(__file__).with_name("web").joinpath("index.html").read_bytes()


def make_handler(runner):
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
                self.json_response(runner.snapshot())
                return
            if parsed.path == "/run":
                query = parse_qs(parsed.query)
                started = runner.start(
                    query.get("text", [DEFAULT_TEXT])[0],
                    query.get("maps", [DEFAULT_MAP_TASKS])[0],
                    query.get("reduces", [DEFAULT_REDUCE_TASKS])[0],
                )
                self.json_response({"ok": True, "started": started})
                return
            self.send_response(404)
            self.end_headers()

        def do_POST(self):
            parsed = urlparse(self.path)
            if parsed.path != "/run":
                self.send_response(404)
                self.end_headers()
                return
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            try:
                payload = json.loads(body.decode("utf-8")) if body else {}
                started = runner.start(
                    payload.get("text", DEFAULT_TEXT),
                    payload.get("maps", DEFAULT_MAP_TASKS),
                    payload.get("reduces", DEFAULT_REDUCE_TASKS),
                )
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                self.json_response({"ok": False, "error": str(exc)}, status=400)
                return
            self.json_response({"ok": True, "started": started})

    return Handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8100)
    parser.add_argument("--ray-address", default="auto")
    parser.add_argument("--dashboard-url", default="")
    args = parser.parse_args()

    ray.init(address=args.ray_address, runtime_env={"working_dir": str(Path(__file__).parent)})
    runner = WordCountRunner(DEFAULT_TEXT, args.dashboard_url)
    runner.start(DEFAULT_TEXT, DEFAULT_MAP_TASKS, DEFAULT_REDUCE_TASKS)
    server = ThreadingHTTPServer((args.host, args.port), make_handler(runner))
    host_ip = socket.gethostbyname(socket.gethostname())
    print(f"Ray Word Count MapReduce: http://{host_ip}:{args.port}")
    if args.dashboard_url:
        print(f"Ray dashboard: {args.dashboard_url}")
    server.serve_forever()


if __name__ == "__main__":
    main()
