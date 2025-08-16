# kvz — Simple ZeroMQ Key/Value Store in Rust

`kvz` is a minimal in-memory key/value store written in Rust.  
It uses [ZeroMQ](https://zeromq.org/) for communication, supports multiple clients, and provides a small benchmark harness to measure latency/throughput.

## Features

- **Key/Value** store:
  - Keys are UTF-8 strings.
  - Values are arbitrary binary blobs with an associated 64-bit timestamp.
- **Semantics**:
  - `PUT` stores `(timestamp, data)` under a key.
  - Replaces existing value only if `new_ts >= old_ts`. Otherwise reply is `STALE`.
  - `GET` fetches the latest value if present, or `MISS` if absent.
- **Protocols**:
  - Requests and replies use ZeroMQ multipart messages (binary safe).
  - Compatible clients can be written in any language with ZMQ bindings.
- **Server options**:
  - `kvz` — simple single-threaded REP server.
  - `kvz-router` — ROUTER/DEALER variant with a worker pool and sharded store for concurrency.
- **Clients**:
  - Built-in CLI subcommands: `put`, `get`, `demo`.
  - Example Python client included (`kvz_client.py`).
- **Benchmark**:
  - `kvz_bench` measures latency distribution and throughput for configurable workloads.

## Build

Requires Rust (1.70+) and ZeroMQ development libraries.

```bash
# Install libzmq (on Debian/Ubuntu)
sudo apt install libzmq3-dev

# Clone and build
git clone https://github.com/jensanjo/kvz.git
cd kvz
cargo build --release
```
## Usage
See [Usage.md](Usage.md)
