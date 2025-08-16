# USAGE.md

Command-line reference for the **kvz** project: the simple server/CLI client (`kvz`),
the worker-pool server (`kvz-router`), and the benchmark tool (`kvz_bench`).

---

## `kvz` — Simple server + CLI client

```
Usage: kvz <COMMAND>

Commands:
  server   Run the server and bind a REP socket
  put      Send a PUT request
  get      Send a GET request
  demo     Quick concurrency demo: spawn N clients doing mixed PUT/GET
  help     Print this message or the help of the given subcommand(s)
```

### `kvz server`

```
Run the server and bind a REP socket

Options:
  --bind <STRING>   Bind endpoint (e.g. tcp://*:5555) [default: tcp://*:5555]
```

### `kvz put`

```
Send a PUT request

Options:
  --connect <STRING>   Endpoint to connect [default: tcp://localhost:5555]
  --key <STRING>       Key (UTF-8)
  --ts <INT>           Timestamp as u64 (e.g. ms since Unix epoch)
  --file <PATH>        Read data from file (if omitted, read from stdin)
```

### `kvz get`

```
Send a GET request

Options:
  --connect <STRING>   Endpoint to connect [default: tcp://localhost:5555]
  --key <STRING>       Key (UTF-8)
  --out <PATH>         Write data to file (if omitted, write to stdout)
```

### `kvz demo`

```
Quick concurrency demo: spawn N clients doing mixed PUT/GET

Options:
  --connect <STRING>   Endpoint [default: tcp://localhost:5555]
  --clients <INT>      Number of client threads [default: 8]
  --iters <INT>        Iterations per client [default: 100]
```

---

## `kvz-router` — Worker-pool server (ROUTER/DEALER)

```
Usage: kvz-router [OPTIONS]

Options:
  --bind <STRING>     Bind endpoint for client connections [default: tcp://*:5555]
  --workers <INT>     Number of worker threads [default: 8]
  --shards <INT>      Number of shards in the in-memory store [default: 64]
```

> Tip: For Unix sockets use an absolute path, e.g. `--bind ipc:///tmp/kvz.sock`
> (ensure the directory exists; remove stale socket files on restart).

---

## `kvz_bench` — Latency/throughput benchmark

```
Usage: kvz_bench [OPTIONS]

Options:
  --connect <STRING>         Endpoint [default: tcp://127.0.0.1:5555]
  --threads <INT>            Number of client threads [default: 8]
  --iters <INT>              Timed iterations per thread [default: 50000]
  --get-ratio <FLOAT>        Fraction of GET ops (0.0–1.0) [default: 0.9]
  --value-size <INT>         Bytes of value payload [default: 256]
  --keys-per-thread <INT>    Number of distinct keys per thread [default: 64]
  --warmup <INT>             Warmup ops per thread (not measured) [default: 5000]
  --csv                      Print per-operation CSV (op,us) to stdout
```

---

## Examples

```bash
# Start single-threaded server (REP):
kvz server --bind tcp://*:5555

# Start worker-pool server with 8 workers and 64 shards:
kvz-router --bind tcp://*:5555 --workers 8 --shards 64

# (IPC on one host)
mkdir -p /tmp
kvz-router --bind ipc:///tmp/kvz.sock --workers 8 --shards 64

# Put a value from stdin (timestamp in ms):
echo "hello" | kvz put --connect tcp://localhost:5555 \
  --key greeting --ts $(date +%s000)

# Get a value and write to a file:
kvz get --connect tcp://localhost:5555 --key greeting --out out.bin

# Demo load: 16 clients × 1000 ops:
kvz demo --connect tcp://localhost:5555 --clients 16 --iters 1000

# Benchmark (TCP):
kvz_bench --connect tcp://127.0.0.1:5555 --threads 8 --iters 500000 \
  --get-ratio 0.9 --value-size 256 --keys-per-thread 64 --warmup 5000

# Benchmark (IPC):
kvz_bench --connect ipc:///tmp/kvz.sock --threads 8 --iters 500000
```
