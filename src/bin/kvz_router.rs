use anyhow::{anyhow, Context, Result};
use clap::Parser;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::{Arc, RwLock};
use std::thread;

/// Server with ROUTER/DEALER + worker pool.
/// Protocol is the same as the simple server:
///   PUT: ["PUT", key(utf8), ts(8B BE), data]
///   GET: ["GET", key]
/// Replies:
///   PUT -> ["OK"] or ["STALE"] or ["ERR", msg]
///   GET -> ["OK", ts(8B BE), data] or ["MISS"] or ["ERR", msg]
#[derive(Parser, Debug)]
#[command(name = "kvz-router")]
#[command(about = "ZeroMQ K/V store (ROUTER/DEALER worker pool)")]
struct Args {
    /// Bind endpoint for client connections (ROUTER)
    #[arg(long, default_value = "tcp://*:5555")]
    bind: String,
    /// Number of worker threads
    #[arg(long, default_value_t = 8)]
    workers: usize,
    /// Number of shards in the in-memory store
    #[arg(long, default_value_t = 64)]
    shards: usize,
}

/// Stored value
#[derive(Clone)]
struct Value {
    ts: u64,
    data: Vec<u8>,
}

/// Very small (fast) hasher for sharding (XOR/shift)
#[derive(Default)]
struct FastHasher(u64);
impl Hasher for FastHasher {
    fn write(&mut self, bytes: &[u8]) {
        // quick & dirty: xor-fold bytes
        let mut h = self.0;
        for &b in bytes {
            h ^= b as u64;
            h = h.rotate_left(5).wrapping_mul(0x9E3779B185EBCA87);
        }
        self.0 = h;
    }
    fn finish(&self) -> u64 {
        self.0
    }
}
type FastBuild = BuildHasherDefault<FastHasher>;

/// Sharded store: Vec<RwLock<HashMap>>
struct ShardedStore {
    shards: Vec<RwLock<HashMap<String, Value, FastBuild>>>,
    mask: usize, // if power-of-two sized, we can mask. Otherwise use modulo.
    pow2: bool,
}

impl ShardedStore {
    fn new(n: usize) -> Self {
        let cap = n.next_power_of_two();
        let pow2 = cap == n;
        let mask = if pow2 { n - 1 } else { 0 };
        let mut shards = Vec::with_capacity(n);
        for _ in 0..n {
            shards.push(RwLock::new(HashMap::with_hasher(FastBuild::default())));
        }
        Self { shards, mask, pow2 }
    }

    #[inline]
    fn shard_index(&self, key: &str) -> usize {
        let mut h = FastHasher(0);
        h.write(key.as_bytes());
        let v = h.finish() as usize;
        if self.pow2 {
            v & self.mask
        } else {
            v % self.shards.len()
        }
    }

    /// PUT semantics: replace only if new_ts >= old_ts. Returns Ok(true) if stored/updated,
    /// Ok(false) if stale. Err on key encoding issues (shouldn't happen here).
    fn put(&self, key: String, ts: u64, data: Vec<u8>) -> Result<bool> {
        let idx = self.shard_index(&key);
        let mut m = self.shards[idx]
            .write()
            .map_err(|_| anyhow!("store poisoned"))?;
        match m.get(&key) {
            Some(v) if ts < v.ts => Ok(false),
            _ => {
                m.insert(key, Value { ts, data });
                Ok(true)
            }
        }
    }

    /// GET: None if miss.
    fn get(&self, key: &str) -> Result<Option<Value>> {
        let idx = self.shard_index(key);
        let m = self.shards[idx]
            .read()
            .map_err(|_| anyhow!("store poisoned"))?;
        Ok(m.get(key).cloned())
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    let ctx = zmq::Context::new();

    // Frontend ROUTER for clients
    let frontend = ctx.socket(zmq::ROUTER)?;
    frontend
        .bind(&args.bind)
        .with_context(|| format!("bind {}", &args.bind))?;

    // Backend DEALER for workers
    let backend = ctx.socket(zmq::DEALER)?;
    let backend_ep = "inproc://kvz-workers";
    backend.bind(backend_ep)?;

    let store = Arc::new(ShardedStore::new(args.shards));
    eprintln!(
        "kvz-router listening on {} with {} workers, {} shards",
        args.bind, args.workers, args.shards
    );

    // Spawn workers
    let mut handles = Vec::with_capacity(args.workers);
    for _ in 0..args.workers {
        let ctx_w = ctx.clone();
        let store_w = Arc::clone(&store);

        handles.push(thread::spawn(move || -> Result<()> {
            let rep = ctx_w.socket(zmq::REP)?;
            rep.connect(backend_ep)?;

            loop {
                let msg = rep.recv_multipart(0)?;
                if msg.is_empty() {
                    send_err(&rep, "empty message")?;
                    continue;
                }
                let cmd = std::str::from_utf8(&msg[0]).unwrap_or("");

                match cmd {
                    "PUT" => {
                        if msg.len() != 4 {
                            send_err(&rep, "PUT expects 4 frames")?;
                            continue;
                        }
                        let key = match String::from_utf8(msg[1].clone()) {
                            Ok(k) => k,
                            Err(_) => {
                                send_err(&rep, "key not utf-8")?;
                                continue;
                            }
                        };
                        if msg[2].len() != 8 {
                            send_err(&rep, "timestamp must be 8 bytes (u64 BE)")?;
                            continue;
                        }
                        let mut tsb = [0u8; 8];
                        tsb.copy_from_slice(&msg[2]);
                        let ts = u64::from_be_bytes(tsb);
                        let data = msg[3].clone();

                        match store_w.put(key, ts, data) {
                            Ok(true) => rep.send_multipart(&[b"OK".as_slice()], 0)?,
                            Ok(false) => rep.send_multipart(&[b"STALE".as_slice()], 0)?,
                            Err(e) => send_err(&rep, &format!("store error: {e}"))?,
                        }
                    }
                    "GET" => {
                        if msg.len() != 2 {
                            send_err(&rep, "GET expects 2 frames")?;
                            continue;
                        }
                        let key = match String::from_utf8(msg[1].clone()) {
                            Ok(k) => k,
                            Err(_) => {
                                send_err(&rep, "key not utf-8")?;
                                continue;
                            }
                        };
                        match store_w.get(&key) {
                            Ok(Some(v)) => {
                                let tsb = v.ts.to_be_bytes();
                                rep.send_multipart(&[b"OK".as_slice(), &tsb, &v.data], 0)?;
                            }
                            Ok(None) => {
                                rep.send_multipart(&[b"MISS".as_slice()], 0)?;
                            }
                            Err(e) => send_err(&rep, &format!("store error: {e}"))?,
                        }
                    }
                    _ => {
                        send_err(&rep, "unknown command")?;
                    }
                }
            }
        }));
    }

    // Start the built-in ZeroMQ proxy: ROUTER <-> DEALER
    // This call blocks and forwards messages between frontend and backend.
    // If you need a clean shutdown, handle a signal and close sockets.
    zmq::proxy(&frontend, &backend).map_err(|e| anyhow!("proxy error: {e}"))?;

    // (Unreachable normally)
    for h in handles {
        let _ = h.join();
    }
    Ok(())
}

fn send_err(sock: &zmq::Socket, msg: &str) -> Result<()> {
    sock.send_multipart(&[b"ERR".as_slice(), msg.as_bytes()], 0)?;
    Ok(())
}
