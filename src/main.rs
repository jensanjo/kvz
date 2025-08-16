use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;

/// Simple ZeroMQ-backed K/V store: in-memory, multi-client, binary-friendly.
#[derive(Parser, Debug)]
#[command(name = "kvz")]
#[command(about = "ZeroMQ K/V store (server + CLI client)")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Run the server and bind a REP socket
    Server {
        /// Bind endpoint, e.g. tcp://*:5555
        #[arg(long, default_value = "tcp://*:5555")]
        bind: String,
    },

    /// Send a PUT request
    Put {
        /// Connect endpoint, e.g. tcp://localhost:5555
        #[arg(long, default_value = "tcp://localhost:5555")]
        connect: String,
        /// Key (UTF-8)
        #[arg(long)]
        key: String,
        /// Timestamp as u64 (e.g. milliseconds since Unix epoch)
        #[arg(long)]
        ts: u64,
        /// Read data from a file (if omitted, reads from stdin)
        #[arg(long)]
        file: Option<PathBuf>,
    },

    /// Send a GET request
    Get {
        /// Connect endpoint, e.g. tcp://localhost:5555
        #[arg(long, default_value = "tcp://localhost:5555")]
        connect: String,
        /// Key (UTF-8)
        #[arg(long)]
        key: String,
        /// Write data to a file (if omitted, writes to stdout)
        #[arg(long)]
        out: Option<PathBuf>,
    },

    /// Quick concurrency demo: spawn N clients doing mixed PUT/GET
    Demo {
        /// Connect endpoint
        #[arg(long, default_value = "tcp://localhost:5555")]
        connect: String,
        /// Number of client threads
        #[arg(long, default_value_t = 8)]
        clients: usize,
        /// Iterations per client
        #[arg(long, default_value_t = 100)]
        iters: usize,
    },
}

#[derive(Clone)]
struct Value {
    ts: u64,
    data: Vec<u8>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Server { bind } => run_server(&bind),
        Cmd::Put { connect, key, ts, file } => client_put(&connect, &key, ts, file),
        Cmd::Get { connect, key, out } => client_get(&connect, &key, out),
        Cmd::Demo { connect, clients, iters } => demo(&connect, clients, iters),
    }
}

fn run_server(bind: &str) -> Result<()> {
    let ctx = zmq::Context::new();
    let socket = ctx.socket(zmq::REP)?;
    socket.bind(bind).with_context(|| format!("bind {}", bind))?;

    // In-memory store
    let mut store: HashMap<String, Value> = HashMap::new();
    eprintln!("kvz server listening on {bind}");

    loop {
        let msg = socket.recv_multipart(0)?;
        if msg.is_empty() {
            send_err(&socket, "empty message")?;
            continue;
        }
        let cmd = std::str::from_utf8(&msg[0]).unwrap_or("");
        match cmd {
            "PUT" => {
                // Expect 4 frames: "PUT", key, ts(8), data
                if msg.len() != 4 {
                    send_err(&socket, "PUT expects 4 frames")?;
                    continue;
                }
                let key = String::from_utf8(msg[1].clone())
                    .map_err(|_| anyhow!("key not utf-8"))?;
                if msg[2].len() != 8 {
                    send_err(&socket, "timestamp must be 8 bytes (u64 BE)")?;
                    continue;
                }
                let mut tsb = [0u8; 8];
                tsb.copy_from_slice(&msg[2]);
                let ts = u64::from_be_bytes(tsb);
                let data = msg[3].clone();

                match store.get(&key) {
                    Some(v) if ts < v.ts => {
                        socket.send_multipart(&[b"STALE".as_slice()], 0)?;
                    }
                    _ => {
                        store.insert(key, Value { ts, data });
                        socket.send_multipart(&[b"OK".as_slice()], 0)?;
                    }
                }
            }
            "GET" => {
                // Expect 2 frames: "GET", key
                if msg.len() != 2 {
                    send_err(&socket, "GET expects 2 frames")?;
                    continue;
                }
                let key = String::from_utf8(msg[1].clone())
                    .map_err(|_| anyhow!("key not utf-8"));
                if key.is_err() {
                    send_err(&socket, "key not utf-8")?;
                    continue;
                }
                let key = key.unwrap();
                if let Some(v) = store.get(&key) {
                    let tsb = v.ts.to_be_bytes();
                    socket.send_multipart(&[b"OK".as_slice(), &tsb, &v.data], 0)?;
                } else {
                    socket.send_multipart(&[b"MISS".as_slice()], 0)?;
                }
            }
            _ => {
                send_err(&socket, "unknown command")?;
            }
        }
    }
}

fn send_err(sock: &zmq::Socket, msg: &str) -> Result<()> {
    sock.send_multipart(&[b"ERR".as_slice(), msg.as_bytes()], 0)?;
    Ok(())
}

fn client_put(connect: &str, key: &str, ts: u64, file: Option<PathBuf>) -> Result<()> {
    let ctx = zmq::Context::new();
    let sock = ctx.socket(zmq::REQ)?;
    sock.connect(connect).with_context(|| format!("connect {}", connect))?;

    let data = match file {
        Some(p) => std::fs::read(p)?,
        None => {
            let mut buf = Vec::new();
            std::io::stdin().read_to_end(&mut buf)?;
            buf
        }
    };

    let tsb = ts.to_be_bytes().to_vec();
    sock.send_multipart(&[b"PUT".as_slice(), key.as_bytes(), &tsb, &data], 0)?;
    let rep = sock.recv_multipart(0)?;
    match rep.first().map(|b| std::str::from_utf8(b).unwrap_or("")) {
        Some("OK") => {
            eprintln!("PUT OK ({} bytes)", data.len());
            Ok(())
        }
        Some("STALE") => {
            eprintln!("PUT STALE (newer value already present)");
            Ok(())
        }
        Some("ERR") => {
            let msg = rep.get(1).and_then(|b| std::str::from_utf8(b).ok()).unwrap_or("");
            Err(anyhow!("PUT ERR: {msg}"))
        }
        other => Err(anyhow!("unexpected reply: {:?}", other)),
    }
}

fn client_get(connect: &str, key: &str, out: Option<PathBuf>) -> Result<()> {
    let ctx = zmq::Context::new();
    let sock = ctx.socket(zmq::REQ)?;
    sock.connect(connect).with_context(|| format!("connect {}", connect))?;

    sock.send_multipart(&[b"GET".as_slice(), key.as_bytes()], 0)?;
    let rep = sock.recv_multipart(0)?;
    if rep.is_empty() {
        return Err(anyhow!("empty reply"));
    }
    match std::str::from_utf8(&rep[0]).unwrap_or("") {
        "OK" => {
            if rep.len() != 3 || rep[1].len() != 8 {
                return Err(anyhow!("malformed OK reply"));
            }
            let mut tsb = [0u8; 8];
            tsb.copy_from_slice(&rep[1]);
            let ts = u64::from_be_bytes(tsb);
            let data = &rep[2];

            eprintln!("GET OK: ts={ts} size={} bytes", data.len());
            match out {
                Some(p) => std::fs::write(p, data)?,
                None => {
                    // Write binary to stdout
                    let mut stdout = std::io::stdout().lock();
                    stdout.write_all(data)?;
                    stdout.flush()?;
                }
            }
            Ok(())
        }
        "MISS" => {
            eprintln!("GET MISS");
            Ok(())
        }
        "ERR" => {
            let msg = rep.get(1).and_then(|b| std::str::from_utf8(b).ok()).unwrap_or("");
            Err(anyhow!("GET ERR: {msg}"))
        }
        other => Err(anyhow!("unexpected reply: {other}")),
    }
}

fn demo(connect: &str, clients: usize, iters: usize) -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    let done = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for id in 0..clients {
        let connect = connect.to_string();
        let done = Arc::clone(&done);
        handles.push(thread::spawn(move || -> Result<()> {
            let ctx = zmq::Context::new();
            let sock = ctx.socket(zmq::REQ)?;
            sock.connect(&connect)?;

            for i in 0..iters {
                // alternate PUT/GET
                let key = format!("key-{}", i % 16);
                let ts = (id as u64) * 1_000_000 + i as u64; // monotonically increasing per client
                let data = format!("hello-from-{}-{}", id, i).into_bytes();

                // PUT
                sock.send_multipart(&[b"PUT".as_slice(), key.as_bytes(), &ts.to_be_bytes(), &data], 0)?;
                let _ = sock.recv_multipart(0)?;

                // GET
                sock.send_multipart(&[b"GET".as_slice(), key.as_bytes()], 0)?;
                let _ = sock.recv_multipart(0)?;
            }
            done.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }));
    }

    for h in handles {
        if let Err(e) = h.join().unwrap_or_else(|_| Err(anyhow!("thread panic"))) {
            eprintln!("demo client error: {e}");
        }
    }
    eprintln!("demo complete: {} clients x {} iters", clients, iters);
    Ok(())
}
