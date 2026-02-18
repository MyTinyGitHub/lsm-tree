# LSM-Tree Storage Engine

A from-scratch implementation of a Log-Structured Merge-tree storage engine in Rust, built to deeply understand the internals of how databases like RocksDB, LevelDB, and Cassandra store data.

## Why I Built This

I'm building a [distributed SQL database engine](../distributed-db) in Rust, and before writing a single line of the database itself I wanted to understand how data is actually stored on disk. Using RocksDB or any other off-the-shelf storage engine would have hidden the most interesting part of the problem.

So I built one from scratch. Every design decision — which compaction strategy to use, whether to add Bloom filters, how to structure the WAL — I made deliberately, which meant I had to understand the tradeoffs behind each choice. That understanding is what I actually needed before building the database layer on top.

This project is the storage backend for each partition node in the [distributed SQL database](../distributed-db).

## Design Decisions & Tradeoffs

Every LSM-tree implementation is a balancing act between three competing pressures: **write amplification**, **read amplification**, and **space amplification**. Improving one typically hurts another. Here's how I navigated those tradeoffs.

### Leveled Compaction over Tiered (STCS)

I chose leveled compaction over Size-Tiered Compaction Strategy (STCS). In leveled compaction, each level has a size limit and SSTables within a level have non-overlapping key ranges, meaning a read needs to check at most one SSTable per level.

**Tradeoff accepted:** Leveled compaction produces higher write amplification than STCS — data is rewritten more frequently as it moves between levels. This is the same tradeoff RocksDB and LevelDB make, prioritizing read performance and space efficiency over raw write throughput.

STCS would have been faster for write-heavy workloads but results in more SSTables overlapping in key range, requiring more files to be checked per read.

### Bloom Filters per SSTable

Each SSTable has an associated Bloom filter. Before doing any disk I/O for a read, I check the filter first — if it reports the key is absent, the SSTable is skipped entirely.

**Tradeoff accepted:** Bloom filters consume memory and have a configurable false positive rate — occasionally they'll indicate a key might exist when it doesn't, causing an unnecessary disk read. In practice this is a clear win: the cost of the occasional false positive is far lower than the cost of checking every SSTable on every read. The false positive rate is tunable via configuration.

This is the same approach used by RocksDB and Cassandra.

### Write-Ahead Log for Durability

Every write is appended to the WAL before the memtable is modified. On crash recovery, the WAL is replayed to restore in-memory state that hadn't been flushed to SSTables yet.

**Tradeoff accepted:** Every write pays a disk I/O cost upfront for the WAL append. This is the standard durability tradeoff — sequential WAL writes are fast, and the guarantee of no data loss on crash is worth it for any serious storage engine.

### XXHash3 for Hashing

I chose XXHash3 over alternatives like MD5, SHA, or FNV for Bloom filter hashing and data integrity checks.

**Why:** XXHash3 is one of the fastest non-cryptographic hash functions available, with excellent distribution properties. Cryptographic hashes are unnecessarily expensive here — collision resistance at a cryptographic level is not required for Bloom filters or integrity checks within a trusted storage engine.

## Architecture

```
Write Path:
  Client → WAL (disk) → MemTable (memory) → [flush] → SSTable (disk)
                                                ↑
                                       Compaction Manager
                                       (background, async)

Read Path:
  Client → MemTable → Immutable MemTable → SSTables (L0 → L1 → ...)
                                              ↑
                                     Bloom Filter check first
                                     Cache (index + filters)
```

### Components

- **MemTable** — In-memory sorted structure for fast writes. Flushed to an SSTable when it reaches the configured size threshold.
- **Immutable MemTable** — A MemTable being flushed to disk. Read-only, allows writes to continue to a fresh MemTable during flush.
- **Write-Ahead Log (WAL)** — Sequential disk log for durability. Replayed on startup after a crash.
- **SSTable** — Sorted String Table. Immutable on-disk file with an associated index and Bloom filter.
- **Bloom Filter** — Per-SSTable probabilistic filter for fast negative lookups.
- **Cache** — In-memory cache for SSTable indexes and Bloom filters, reducing disk reads on hot paths.
- **Compaction Manager** — Background async process that merges and rewrites SSTables according to the leveled compaction strategy.
- **Manifest** — Tracks metadata about all SSTables and their level assignments.

## Quick Start

```bash
cargo build --release

# Run the interactive CLI
cargo run

# Available commands:
# add     - Add a key-value pair
# get     - Retrieve a value by key
# delete  - Delete a key (uses tombstone)
# print   - Print current tree state
# exit    - Exit the program
```

## Configuration

```toml
[memtable]
max_entries = 5  # Flush to disk after this many entries

[cache]
index_size = 5           # Cache size for indexes
bloom_filter_size = 50   # Bloom filter capacity

[ss_table]
l0_file_count_limit = 3  # Trigger compaction at this level
l1_file_size_upper_limit = 1000
```

Configuration files: `config.dev.toml`, `config.test.toml`, `config.prod.toml`

## Testing

```bash
cargo test
RUST_LOG=info cargo test   # with logging
cargo test test_recreating # specific test
```

The test suite covers crash recovery scenarios, data consistency guarantees, compaction behavior, and CRUD operations.

## Project Structure

```
src/
├── structures/
│   ├── lsm.rs                 # Main LSM-tree implementation
│   ├── memtable.rs            # In-memory table
│   ├── write_ahead_logger.rs  # WAL for durability
│   ├── ss_table_manager.rs    # Persistent storage
│   ├── cache.rs               # Index and bloom filter cache
│   ├── compaction_manager.rs  # Background compaction
│   ├── manifest.rs            # Metadata tracking
│   └── bloom_filter.rs        # Probabilistic filter
├── config.rs                  # Configuration management
├── error.rs                   # Error types
└── main.rs                    # CLI interface
```

## Requirements

- Rust 2024 edition
- `tokio` — Async runtime
- `serde` — Serialization
- `xxhash-rust` — Fast hashing
- `log4rs` — Structured logging
- `thiserror` — Error handling

## What I Learned

Building this made the abstract tradeoffs in storage engine design concrete. The decision between leveled and tiered compaction stopped being theoretical once I could see the difference in how many SSTables accumulated and how reads performed at each level. The Bloom filter false positive rate became tangible when tuning it against memory usage. Fighting Rust's borrow checker throughout forced a much deeper understanding of ownership and concurrency than any higher-level language would have required.

This project directly informs the storage layer of the [distributed SQL database engine](../distributed-db) — the same LSM internals, now serving as the per-partition storage backend in a distributed system.
