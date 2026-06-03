# Ray Word Count MapReduce Demo

This demo implements the assignment-style Word Count MapReduce pipeline with Ray tasks and a web UI.

Core functions are in `app.py`:

- `map_function(map_input)` emits lowercase `(word, 1)` pairs.
- `do_map_task(map_input, num_reduce_tasks)` emits one intermediate bucket per reduce task.
- `reduce_function(vals)` returns the number of values for a word.
- `do_reduce_task(input_buckets)` groups words and returns `{word: count}`.

Run it against the existing Ray cluster:

```bash
cd word_count_mr
./start.sh
```

Useful environment variables:

```bash
PORT=8100 RAY_ADDRESS=auto DASHBOARD_URL=http://<head-ip>:8265 ./start.sh
```

Run the MapReduce correctness test:

```bash
cd word_count_mr
./test_mr.sh
```
