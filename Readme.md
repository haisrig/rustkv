# rustkv 🦀

A Redis-compatible in-memory key-value store built from scratch in Rust.

Built as a learning project to explore async Rust, Tokio, and systems programming concepts.

---

## Features

- **RESP Protocol** — Fully compatible with `redis-cli` and Redis clients
- **Core Commands** — `SET`, `GET`, `DEL`, `PING`
- **TTL / Key Expiry** — Set expiry on keys with `EX` option
- **Pub/Sub Messaging** — `PUBLISH` and `SUBSCRIBE` with broadcast channels
- **AOF Persistence** — Append-Only File for data recovery on restart
- **Concurrent Clients** — Handles multiple clients simultaneously via Tokio async tasks

---

## Tech Stack

| Concept | Implementation |
|---|---|
| Async runtime | `tokio` with multi-thread scheduler |
| Shared state | `Arc<Mutex<HashMap>>` |
| Pub/Sub | `tokio::sync::broadcast` channels |
| Key expiry | `tokio::time::interval` background task |
| Persistence | Append-Only File with async replay |
| Protocol | RESP (Redis Serialization Protocol) |

---

## Getting Started

### Prerequisites

- Rust (stable) — [install via rustup](https://rustup.rs)
- `redis-cli` for testing (optional)

### Run

```bash
git clone https://github.com/SrikanthGunuputi/rustkv
cd rustkv
cargo run
```

Server starts on `127.0.0.1:6379` — same default port as Redis.

---

## Usage

### Basic Commands

```bash
# Ping
redis-cli -p 6379 ping
# → PONG

# Set a key
redis-cli -p 6379 set name Srikanth
# → OK

# Get a key
redis-cli -p 6379 get name
# → Srikanth

# Delete a key
redis-cli -p 6379 del name
# → OK

# Get non-existent key
redis-cli -p 6379 get name
# → (nil)
```

### TTL / Key Expiry

```bash
# Set key with 20 second expiry
redis-cli -p 6379 set city Hyderabad ex 20

# Key available before expiry
redis-cli -p 6379 get city
# → Hyderabad

# Key gone after expiry
redis-cli -p 6379 get city
# → (nil)
```

### Pub/Sub

```bash
# Terminal 1 — subscribe to a channel
redis-cli -p 6379 subscribe news

# Terminal 2 — publish a message
redis-cli -p 6379 publish news "Hello from rustkv!"

# Terminal 1 receives:
# 1) "message"
# 2) "news"
# 3) "Hello from rustkv!"
```

### AOF Persistence

Data survives server restarts automatically:

```bash
# Set some keys
redis-cli -p 6379 set name Srikanth
redis-cli -p 6379 set city Hyderabad

# Restart the server (Ctrl+C, cargo run)
# AOF replay complete — 2 commands restored

# Keys still available
redis-cli -p 6379 get name   # → Srikanth
redis-cli -p 6379 get city   # → Hyderabad
```

---

## Architecture

```
Client Connection
       ↓
TcpListener (tokio)
       ↓
tokio::spawn → handle_client (one task per client)
       ↓
parse_command() → RESP parser → Command enum
       ↓
 ┌─────────────────────────────────┐
 │         Shared State            │
 │  Arc<Mutex<HashMap>> (Store)    │  ← GET / SET / DEL
 │  Arc<Mutex<HashMap>> (PubSub)   │  ← PUBLISH / SUBSCRIBE
 │  Arc<Mutex<File>>   (AOF)       │  ← Persistence
 └─────────────────────────────────┘
       ↓
Background Tasks
  ├── handle_key_expiry  (runs every 10s, removes expired keys)
  └── replay_aof         (runs once on startup, restores state)
```

---

## Project Structure

```
rustkv/
├── src/
│   └── main.rs       # All server logic
├── backup.txt        # AOF persistence file (auto-created)
├── Cargo.toml
└── README.md
```

---

## Key Rust Concepts Demonstrated

| Concept | Where used |
|---|---|
| `async/await` | All I/O operations |
| `tokio::spawn` | Per-client task spawning |
| `Arc<Mutex<T>>` | Shared state across async tasks |
| `broadcast::channel` | Pub/Sub message delivery |
| Enum with data | `Command` variants carry typed arguments |
| RAII / Drop | Lock release via MutexGuard scope |
| Pattern matching | RESP parsing, command dispatch |
| `Option<T>` | Optional TTL on entries |
| `Result<T, E>` | Error handling throughout |
| Trait bounds | `AsyncReadExt`, `AsyncWriteExt` |

---

## Limitations

- Single node only — no clustering
- No authentication
- AOF replay does not restore TTL on keys
- Pub/Sub subscribers limited to 16 buffered messages per channel
- Not all Redis commands implemented

---

## Roadmap

- [ ] `EXPIRE` command (set TTL on existing key)
- [ ] `KEYS` pattern matching
- [ ] Replace `Arc<Mutex<HashMap>>` with `DashMap` for better concurrency
- [ ] Single-threaded event loop (closer to real Redis architecture)
- [ ] Benchmark with `redis-benchmark`
- [ ] TLS support

---

## Author

**Srikanth Gunuputi**  
Senior Software Engineer & AWS Cloud Architect  
[GitHub](https://github.com/SrikanthGunuputi) · [LinkedIn](https://linkedin.com/in/srikanth-gunuputi)

---

## License

MIT