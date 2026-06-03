#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
../.venv/bin/python - <<'PY'
from collections import Counter

import ray

from app import do_map_task, do_reduce_task, map_function, reduce_function, split_text


text = "Ray ray Map MAP reduce, reduce task!"
expected = Counter({"ray": 2, "map": 2, "reduce": 2, "task": 1})

assert map_function("Hello hello") == [("hello", 1), ("hello", 1)]
assert reduce_function([1, 1, 1]) == 3

ray.init(address="auto", ignore_reinit_error=True, runtime_env={"working_dir": "."})
try:
    num_maps = 3
    num_reduces = 2
    map_inputs = split_text(text, num_maps)
    map_refs = [
        list(do_map_task.options(num_returns=num_reduces).remote(map_input, num_reduces))
        for map_input in map_inputs
    ]
    map_outputs = [ray.get(refs) for refs in map_refs]
    reduce_inputs = [
        [map_outputs[map_id][reduce_id] for map_id in range(num_maps)]
        for reduce_id in range(num_reduces)
    ]
    reduce_outputs = ray.get([
        do_reduce_task.remote(reduce_inputs[reduce_id])
        for reduce_id in range(num_reduces)
    ])
    actual = Counter()
    for partial in reduce_outputs:
        assert partial["node_ip"]
        actual.update(partial["output"])

    for outputs_by_reduce in map_outputs:
        for bucket in outputs_by_reduce:
            assert bucket["node_ip"]
            assert "pairs" in bucket

    assert actual == expected, f"expected {expected}, got {actual}"
    print("WordCount MapReduce test passed")
finally:
    ray.shutdown()
PY
