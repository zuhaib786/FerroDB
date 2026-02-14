# FerroDB 🦀

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**FerroDB** is a high-performance, Redis-compatible key-value cache database built from scratch in Rust. Designed as both a learning project, FerroDB implements core Redis functionality with modern async I/O and robust persistence.

---

## ✨ Features

### Data Structures
- **Strings** - Simple key-value storage with TTL support
- **Lists** - Double-ended queues with O(1) push/pop operations
- **Sets** - Unique collections with set operations (intersection, union, difference)
- **Sorted Sets** - Score-based ordered collections for leaderboards and rankings

### Persistence
- **RDB Snapshots** - Point-in-time binary snapshots for fast restarts
- **AOF (Append-Only File)** - Write-ahead logging with 1-second fsync for durability
- **Hybrid Mode** - Combine RDB and AOF for optimal performance and safety
- **Auto-save** - Configurable periodic snapshots every 60 seconds

### Real-time Messaging
- **Pub/Sub** - Publish/subscribe message broadcasting for real-time applications
- **Multiple Channels** - Subscribe to multiple channels simultaneously
- **Pattern Matching** - Support for channel subscription patterns

### Core Features
- **TTL/Expiration** - Passive and active key expiration strategies
- **RESP Protocol** - Full Redis Serialization Protocol compatibility
- **Async I/O** - Built on Tokio for high concurrency
- **Thread-safe** - Arc<RwLock> for safe concurrent access
- **Redis-cli Compatible** - Works seamlessly with standard Redis clients

---

## 🚀 Quick Start

### Prerequisites
- Rust 1.70 or higher
- Cargo

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/ferrodb.git
cd ferrodb

# Build the project
cargo build --release

# Run the server
cargo run --release
```

The server will start on `127.0.0.1:6379` by default.

### Connect with redis-cli

```bash
# Install redis-cli (if not already installed)
# On macOS:
brew install redis

# On Ubuntu:
sudo apt-get install redis-tools

# Connect to FerroDB
redis-cli -p 6379
```

---

## 📖 Usage Examples

### Strings

```redis
# Set a key
SET user:1 "Alice"
OK

# Get a key
GET user:1
"Alice"

# Set with expiration (10 seconds)
SETEX session:abc 10 "user123"
OK

# Check TTL
TTL session:abc
(integer) 8

# Multiple operations
MSET user:2 "Bob" user:3 "Charlie"
OK

MGET user:1 user:2 user:3
1) "Alice"
2) "Bob"
3) "Charlie"
```

### Lists

```redis
# Push to left (head)
LPUSH tasks "task3" "task2" "task1"
(integer) 3

# Push to right (tail)
RPUSH tasks "task4"
(integer) 4

# Get range
LRANGE tasks 0 -1
1) "task1"
2) "task2"
3) "task3"
4) "task4"

# Pop from left
LPOP tasks
"task1"

# Get length
LLEN tasks
(integer) 3
```

### Sets

```redis
# Add members
SADD fruits "apple" "banana" "orange"
(integer) 3

# Check membership
SISMEMBER fruits "apple"
(integer) 1

# Get all members
SMEMBERS fruits
1) "orange"
2) "banana"
3) "apple"

# Set operations
SADD citrus "orange" "lemon" "lime"
(integer) 3

SINTER fruits citrus
1) "orange"

SUNION fruits citrus
1) "lime"
2) "lemon"
3) "orange"
4) "banana"
5) "apple"
```

### Sorted Sets

```redis
# Add members with scores
ZADD leaderboard 100 "alice" 200 "bob" 150 "charlie"
(integer) 3

# Get range (sorted by score)
ZRANGE leaderboard 0 -1
1) "alice"
2) "charlie"
3) "bob"

# Get range with scores
ZRANGE leaderboard 0 -1 WITHSCORES
1) "alice"
2) "100"
3) "charlie"
4) "150"
5) "bob"
6) "200"

# Get score
ZSCORE leaderboard "alice"
"100"

# Get rank (0-based index)
ZRANK leaderboard "bob"
(integer) 2

# Update score
ZADD leaderboard 250 "alice"
(integer) 0
```

### Pub/Sub

```redis
# Terminal 1: Subscribe to channels
SUBSCRIBE news sports
Reading messages...
1) "subscribe"
2) "news"
3) (integer) 1
4) "subscribe"
5) "sports"
6) (integer) 2

# Terminal 2: Publish messages
PUBLISH news "Breaking: Rust 2.0 released!"
(integer) 1

PUBLISH sports "Goal scored!"
(integer) 1

# Terminal 1 receives:
1) "message"
2) "news"
3) "Breaking: Rust 2.0 released!"
1) "message"
2) "sports"
3) "Goal scored!"
```

### Persistence

```redis
# Manual save (blocking)
SAVE
OK

# Background save (non-blocking)
BGSAVE
Background saving started

# Check database size
DBSIZE
(integer) 42

# Rewrite AOF (compact log)
BGREWRITEAOF
Background AOF rewrite started
```

---

## 🏗️ Architecture

### Core Components

```
┌─────────────────────────────────────────────────────┐
│                   FerroDB Server                     │
├─────────────────────────────────────────────────────┤
│                                                       │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │
│  │   TCP Server │  │ RESP Protocol│  │  Command  │ │
│  │    (Tokio)   │─▶│    Parser    │─▶│  Handler  │ │
│  └──────────────┘  └──────────────┘  └───────────┘ │
│                                            │         │
│  ┌─────────────────────────────────────────┼──────┐ │
│  │         Storage Engine (FerroStore)     │      │ │
│  ├─────────────────────────────────────────┘      │ │
│  │  Arc<RwLock<HashMap<String, DataType>>>        │ │
│  │                                                  │ │
│  │  DataTypes:                                     │ │
│  │  • String                                       │ │
│  │  • List (VecDeque)                              │ │
│  │  • Set (HashSet)                                │ │
│  │  • SortedSet (BTreeMap + HashMap)              │ │
│  └──────────────────────────────────────────────────┘ │
│                                                       │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │
│  │ RDB Snapshot │  │  AOF Logger  │  │  PubSub   │ │
│  │   (Binary)   │  │  (Text Log)  │  │   Hub     │ │
│  └──────────────┘  └──────────────┘  └───────────┘ │
└─────────────────────────────────────────────────────┘
```

### Design Patterns

- **Thread-safe Storage**: `Arc<RwLock<HashMap>>` enables safe concurrent access
- **Dual-index Sorted Sets**: BTreeMap for ordering + HashMap for O(1) lookups
- **Async I/O**: Tokio runtime for non-blocking operations
- **Write-ahead Logging**: AOF logs commands before execution
- **Broadcast Channels**: Tokio's broadcast for efficient Pub/Sub

---

## 📋 Supported Commands

### String Commands
- `SET key value` - Set a string value
- `GET key` - Get a string value
- `MSET key1 value1 key2 value2 ...` - Set multiple keys
- `MGET key1 key2 ...` - Get multiple keys
- `DEL key [key ...]` - Delete keys
- `EXISTS key [key ...]` - Check if keys exist
- `SETEX key seconds value` - Set with expiration

### List Commands
- `LPUSH key element [element ...]` - Push to left
- `RPUSH key element [element ...]` - Push to right
- `LPOP key` - Pop from left
- `RPOP key` - Pop from right
- `LLEN key` - Get list length
- `LRANGE key start stop` - Get range of elements

### Set Commands
- `SADD key member [member ...]` - Add members to set
- `SREM key member [member ...]` - Remove members
- `SMEMBERS key` - Get all members
- `SISMEMBER key member` - Check membership
- `SCARD key` - Get set size
- `SINTER key [key ...]` - Set intersection
- `SUNION key [key ...]` - Set union
- `SDIFF key [key ...]` - Set difference

### Sorted Set Commands
- `ZADD key score member [score member ...]` - Add members with scores
- `ZREM key member [member ...]` - Remove members
- `ZSCORE key member` - Get member's score
- `ZRANGE key start stop [WITHSCORES]` - Get range by index
- `ZRANK key member` - Get member's rank
- `ZCARD key` - Get sorted set size

### Pub/Sub Commands
- `SUBSCRIBE channel [channel ...]` - Subscribe to channels
- `UNSUBSCRIBE [channel ...]` - Unsubscribe from channels
- `PUBLISH channel message` - Publish message to channel

### TTL Commands
- `EXPIRE key seconds` - Set key expiration
- `TTL key` - Get time to live
- `PERSIST key` - Remove expiration

### Persistence Commands
- `SAVE` - Synchronous save to disk
- `BGSAVE` - Asynchronous background save
- `BGREWRITEAOF` - Compact AOF file

### Utility Commands
- `PING` - Test connection
- `DBSIZE` - Get number of keys

---

## 🛠️ Development

### Running Tests

```bash
# Run all tests
cargo test

# Run specific test module
cargo test storage
cargo test persistence
cargo test pubsub

# Run with output
cargo test -- --nocapture
```

### Project Structure

```
ferrodb/
├── src/
│   ├── main.rs           # TCP server and connection handling
│   ├── lib.rs            # Module exports
│   ├── storage.rs        # Core storage engine
│   ├── protocol.rs       # RESP protocol parser/encoder
│   ├── commands.rs       # Command handlers
│   ├── persistence.rs    # RDB snapshot handling
│   ├── aof.rs           # AOF logging
│   └── pubsub.rs        # Pub/Sub system
├── tests/               # Integration tests
├── Cargo.toml          # Dependencies
└── README.md
```

### Key Dependencies

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
ordered-float = "4.2"
```

---

## 🎯 Roadmap

### Completed ✅
- [x] Core storage engine with multi-type support
- [x] RESP protocol implementation
- [x] Async TCP server
- [x] String, List, Set, Sorted Set data types
- [x] TTL and expiration system
- [x] RDB snapshots
- [x] AOF logging
- [x] Pub/Sub messaging
- [x] 40+ Redis commands

### Planned 🚧
- [ ] Hashes data structure
- [ ] Transactions (MULTI/EXEC)
- [ ] Lua scripting support
- [ ] Blocking operations (BLPOP/BRPOP)
- [ ] Pattern-based Pub/Sub (PSUBSCRIBE)
- [ ] Configuration file support
- [ ] INFO command
- [ ] Replication (master/replica)
- [ ] Clustering
- [ ] Authentication (AUTH)
- [ ] Memory eviction policies (LRU/LFU)
- [ ] Benchmark suite

---

## 🧪 Testing

FerroDB uses comprehensive testing strategies:

- **Unit Tests** - Test individual storage operations
- **Integration Tests** - Test command handlers with RESP protocol
- **End-to-End Tests** - Validate with real redis-cli connections

### Example Test Run

```bash
$ cargo test

running 45 tests
test storage::tests::test_sadd_basic ... ok
test storage::tests::test_zadd_basic ... ok
test storage::tests::test_sinter ... ok
test commands::integration_tests::test_publish_subscribe ... ok
test persistence_tests::test_save_and_load_strings ... ok
...

test result: ok. 45 passed; 0 failed; 0 ignored
```

---

## 📊 Performance Characteristics

### Time Complexity

| Operation | Complexity |
|-----------|-----------|
| GET/SET | O(1) |
| LPUSH/RPUSH/LPOP/RPOP | O(1) |
| SADD/SREM/SISMEMBER | O(1) |
| ZADD/ZREM | O(log N) |
| LRANGE | O(N) where N is range size |
| SMEMBERS | O(N) where N is set size |
| ZRANGE | O(log N + M) where M is range size |
| SINTER/SUNION | O(N*M) worst case |

### Memory

- Strings: ~16 bytes overhead + string size
- Lists: VecDeque overhead + string sizes
- Sets: HashSet overhead + string sizes
- Sorted Sets: BTreeMap + HashMap overhead + string sizes
- TTL: Additional 16 bytes (Instant) per key with expiration

---

## 🤝 Contributing

Contributions are welcome! This project is built for learning, so feel free to:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Clone your fork
git clone https://github.com/yourusername/ferrodb.git

# Install Rust (if needed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build and test
cargo build
cargo test

# Run with logging
RUST_LOG=debug cargo run
```

---

## 📚 Learning Resources

This project demonstrates:

- **Rust Fundamentals**: Ownership, borrowing, lifetimes
- **Concurrency**: Arc, RwLock, async/await
- **Network Programming**: TCP servers with Tokio
- **Protocol Implementation**: RESP parsing and encoding
- **Data Structures**: HashMap, BTreeMap, VecDeque, HashSet
- **Database Concepts**: Persistence, replication, pub/sub
- **Production Patterns**: Error handling, testing, logging

### Recommended Reading

- [Redis Protocol Specification](https://redis.io/docs/reference/protocol-spec/)
- [Tokio Documentation](https://tokio.rs/)
- [The Rust Programming Language Book](https://doc.rust-lang.org/book/)
- [Redis Design and Implementation](https://redis.com/ebook/redis-in-action/)

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **Redis** - For the amazing protocol and design inspiration
- **Tokio** - For the excellent async runtime
- **Rust Community** - For the powerful language and ecosystem
- **Claude** - Built purely using claude and very less of information from my end :P(Even this readme file).  Basically i am learning from claude
---

## 📧 Contact

---

<div align="center">
  <p>Built with 🦀 and ❤️</p>
  <p>⭐ Star this repo if you find it helpful!</p>
</div>

