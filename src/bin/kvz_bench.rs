use anyhow::{anyhow, Context, Result};
use clap::Parser;
use rand::{distributions::Alphanumeric, rngs::StdRng, Rng, SeedableRng};
// use std::cmp::Ordering;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser, Debug, Clone)]
#[command(about = "Latency benchmark for kvz ZeroMQ K/V store")]
struct Args {
    /// Endpoint to connect to (REQ)
    #[arg(long, default_value = "tcp://127.0.0.1:5555")]
    connect: String,
    /// Number of client threads
    #[arg(long, default_value_t = 8)]
    threads: usize,
    /// Timed iterations per thread
    #[arg(long, default_value_t = 50_000)]
    iters: usize,
    /// Fraction of GET ops (0.0..=1.0). e.g. 0.9 = 90% GET, 10% PUT
    #[arg(long, default_value_t = 0.9)]
    get_ratio: f64,
    /// Bytes of value payload
    #[arg(long, default_value_t = 256)]
    value_size: usize,
    /// Number of distinct keys per thread (reduce contention)
    #[arg(long, default_value_t = 64)]
    keys_per_thread: usize,
    /// Warmup ops per thread (not measured)
    #[arg(long, default_value_t = 5_000)]
    warmup: usize,
    /// Print per-op CSV (op,us) to stdout
    #[arg(long)]
    csv: bool,
}

#[derive(Default, Clone)]
struct Stats {
    lat_us: Vec<u32>, // microseconds per op
    puts: usize,
    gets: usize,
}

impl Stats {
    fn merge(mut self, mut other: Stats) -> Stats {
        self.lat_us.append(&mut other.lat_us);
        self.puts += other.puts;
        self.gets += other.gets;
        self
    }

    fn summarize(&mut self) -> Summary {
        self.lat_us.sort_unstable();
        let n = self.lat_us.len().max(1);
        let idx = |q: f64| -> usize {
            let p = (q * (n as f64 - 1.0)).clamp(0.0, (n - 1) as f64).round() as usize;
            p
        };
        let p50 = self.lat_us[idx(0.50)];
        let p95 = self.lat_us[idx(0.95)];
        let p99 = self.lat_us[idx(0.99)];
        let p999 = self.lat_us[idx(0.999)];
        let p9999 = self.lat_us[idx(0.9999)];
        let max = *self.lat_us.last().unwrap();
        let avg = (self.lat_us.iter().map(|&x| x as u64).sum::<u64>() as f64) / n as f64;
        Summary { p50, p95, p99, p999, p9999, max, avg_us: avg, puts: self.puts, gets: self.gets, count: n }
    }
}

struct Summary {
    p50: u32,
    p95: u32,
    p99: u32,
    p999: u32,
    p9999: u32,
    max: u32,
    avg_us: f64,
    puts: usize,
    gets: usize,
    #[allow(dead_code)]
    count: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !(0.0..=1.0).contains(&args.get_ratio) {
        return Err(anyhow!("--get-ratio must be between 0.0 and 1.0"));
    }

    // One context shared across threads (as recommended by ZeroMQ)
    let ctx = Arc::new(zmq::Context::new());
    let barrier = Arc::new(Barrier::new(args.threads));
    let start_barrier = Arc::new(Barrier::new(args.threads));

    // Pre-generate value payload (same size; content varies per thread to avoid dedup illusions)
    let mut handles = Vec::with_capacity(args.threads);
    let bench_start = Instant::now();

    for tid in 0..args.threads {
        let ctx = Arc::clone(&ctx);
        let barrier = Arc::clone(&barrier);
        let start_barrier = Arc::clone(&start_barrier);
        let args = args.clone();

        handles.push(thread::spawn(move || -> Result<Stats> {
            let sock = ctx.socket(zmq::REQ)?;
            sock.connect(&args.connect).with_context(|| format!("connect {}", args.connect))?;

            // Thread-local RNG and data buffer
            let mut rng = StdRng::seed_from_u64(0xC0FFEE + tid as u64);
            let mut value = vec![0u8; args.value_size];
            rng.fill(&mut value[..]);

            // Build key set for this thread
            let rand_suffix: String = (0..6)
                .map(|_| rng.sample(Alphanumeric) as char)
                .collect();
            let key_prefix: String = format!(
                "bench-{}-{}-",
                tid,
                rand_suffix
            );
            let keys: Vec<String> = (0..args.keys_per_thread)
                .map(|i| format!("{}{}", key_prefix, i))
                .collect();

            // Preload/warm keys with PUT so GETs will hit
            let base_ts = now_millis();
            for i in 0..args.warmup {
                let k = &keys[i % keys.len()];
                let ts = base_ts + i as u64;
                zmq_put(&sock, k, ts, &value)?;
                if i % 128 == 0 {
                    // mutate payload a bit
                    let j = rng.gen_range(0..value.len());
                    value[j] ^= (i as u8).wrapping_mul(31);
                }
            }

            // Sync start across threads; also give server a moment to drain warmup
            barrier.wait();
            thread::sleep(Duration::from_millis(50));
            start_barrier.wait();

            // Timed run
            let mut stats = Stats::default();
            stats.lat_us.reserve(args.iters);

            let mut ts_counter = base_ts + args.warmup as u64;
            let mut value = value; // reuse
            for i in 0..args.iters {
                // Choose op by ratio (deterministic via RNG)
                let do_get = rng.gen_bool(args.get_ratio);
                let key = &keys[i % keys.len()];

                let t0 = Instant::now();
                if do_get {
                    let (_ts, _data) = match zmq_get(&sock, key)? {
                        Some(x) => x,
                        None => {
                            // On MISS (shouldn't happen), do a PUT to seed it
                            zmq_put(&sock, key, ts_counter, &value)?;
                            (ts_counter, value.clone())
                        }
                    };
                } else {
                    ts_counter += 1;
                    // small mutation to avoid identical payloads
                    if !value.is_empty() {
                        let pos = (i + tid) % value.len();
                        value[pos] ^= (i as u8).wrapping_mul(13);
                    }
                    let rep = zmq_put(&sock, key, ts_counter, &value)?;
                    // If server says STALE (clock skew), bump ts and retry once (not timed separately)
                    if matches!(rep, PutReply::Stale) {
                        ts_counter += 1;
                        zmq_put(&sock, key, ts_counter, &value)?;
                    }
                    stats.puts += 1;
                }
                let dt = t0.elapsed();
                stats.lat_us.push(duration_to_us(dt) as u32);
                if do_get {
                    stats.gets += 1;
                }
            }

            Ok(stats)
        }));
    }

    // Collect results
    let mut agg = Stats::default();
    let timed_span = {
        let t0 = Instant::now();
        for h in handles {
            match h.join() {
                Ok(Ok(s)) => agg = agg.merge(s),
                Ok(Err(e)) => eprintln!("worker error: {e:?}"),
                Err(_) => eprintln!("worker panic"),
            }
        }
        t0.elapsed()
    };

    // Summary
    let total_ops = agg.lat_us.len();
    let wall = bench_start.elapsed();
    let throughput = (total_ops as f64) / wall.as_secs_f64();
    let timed_throughput = (total_ops as f64) / timed_span.as_secs_f64();
    let sum = agg.summarize();

    println!("== kvz latency benchmark ==");
    println!("endpoint       : {}", args.connect);
    println!("threads        : {}", args.threads);
    println!("iters/thread   : {}", args.iters);
    println!("get_ratio      : {:.3}", args.get_ratio);
    println!("value_size     : {} B", args.value_size);
    println!("keys/thread    : {}", args.keys_per_thread);
    println!("warmup/thread  : {}", args.warmup);
    println!();
    println!("ops total      : {}", total_ops);
    println!("ops GET/PUT    : {}/{}", sum.gets, sum.puts);
    println!("throughput     : {:>8.0} ops/s (total wall)", throughput);
    println!("throughput     : {:>8.0} ops/s (timed section)", timed_throughput);
    println!(
        "latency (us)   : p50 {:>6}  p95 {:>6}  p99 {:>6}  p99.9 {:-6} p999.9 {:>6} max {:>6}  avg {:>7.1}",
        sum.p50, sum.p95, sum.p99, sum.p999, sum.p9999, sum.max, sum.avg_us
    );

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PutReply {
    Ok,
    Stale,
}

fn zmq_put(sock: &zmq::Socket, key: &str, ts: u64, data: &[u8]) -> Result<PutReply> {
    sock.send_multipart(&[b"PUT".as_slice(), key.as_bytes(), &ts.to_be_bytes(), data], 0)?;
    let rep = sock.recv_multipart(0)?;
    if rep.is_empty() {
        return Err(anyhow!("empty PUT reply"));
    }
    match std::str::from_utf8(&rep[0]).unwrap_or("") {
        "OK" => Ok(PutReply::Ok),
        "STALE" => Ok(PutReply::Stale),
        "ERR" => {
            let msg = rep.get(1).and_then(|b| std::str::from_utf8(b).ok()).unwrap_or("");
            Err(anyhow!("PUT ERR: {msg}"))
        }
        x => Err(anyhow!("unexpected PUT reply: {x:?}")),
    }
}

fn zmq_get(sock: &zmq::Socket, key: &str) -> Result<Option<(u64, Vec<u8>)>> {
    sock.send_multipart(&[b"GET".as_slice(), key.as_bytes()], 0)?;
    let rep = sock.recv_multipart(0)?;
    if rep.is_empty() {
        return Err(anyhow!("empty GET reply"));
    }
    match std::str::from_utf8(&rep[0]).unwrap_or("") {
        "MISS" => Ok(None),
        "OK" => {
            if rep.len() != 3 || rep[1].len() != 8 {
                return Err(anyhow!("malformed OK reply"));
            }
            let mut tsb = [0u8; 8];
            tsb.copy_from_slice(&rep[1]);
            let ts = u64::from_be_bytes(tsb);
            Ok(Some((ts, rep[2].clone())))
        }
        "ERR" => {
            let msg = rep.get(1).and_then(|b| std::str::from_utf8(b).ok()).unwrap_or("");
            Err(anyhow!("GET ERR: {msg}"))
        }
        x => Err(anyhow!("unexpected GET reply: {x:?}")),
    }
}

#[inline]
fn now_millis() -> u64 {
    // wall-clock is fine for monotonic-ish stamping here
    (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO))
    .as_millis() as u64
}

#[inline]
fn duration_to_us(d: Duration) -> u128 {
    (d.as_secs() as u128) * 1_000_000 + (d.subsec_nanos() as u128) / 1_000
}
