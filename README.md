# LSM-Tree Implementation

A high-performance, persistent key-value storage engine built with Rust, implementing a Log-Structured Merge-tree with durability and concurrency.

## Core Strengths

- **🚀 Production-Ready**: Complete LSM-tree implementation with Write-Ahead Logging for crash recovery
- **⚡ High Performance**: Async compaction, Bloom filters, and multi-level caching for optimal read/write performance  
- **🔄 Concurrency**: Built on Tokio with background compaction and thread-safe data structures
- **🛡️ Durability**: Write-Ahead Logging ensures no data loss on crashes with automatic recovery
- **⚙️ Configurable**: TOML-based configuration supporting multiple environments (dev/test/prod)
- **🎯 Clean Architecture**: Modular design with clear separation of concerns
- **🖥️ Interactive CLI**: User-friendly command-line interface for testing and exploration
- **✅ Well Tested**: Comprehensive test suite ensuring correctness and reliability

## Quick Start

```bash
# Clone and build
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

## Architecture

The implementation follows the classic LSM-tree design with these core components:

- **MemTable**: In-memory sorted data structure for fast writes
- **Immutable MemTable**: Read-only memory table being flushed to disk
- **Write-Ahead Log (WAL)**: Durability layer for crash recovery
- **SSTable**: Sorted String Table for persistent storage
- **Bloom Filter**: Probabilistic data structure for fast negative lookups
- **Cache**: Multi-level caching with indexes and bloom filters
- **Compaction Manager**: Background process optimizing storage layout
- **Manifest**: Tracks metadata about all SSTables

## Configuration

The system uses environment-based TOML configuration:

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

## Features

### Performance Optimizations
- **Level-based compaction**: Leveled compaction strategy with configurable thresholds
- **Cache layer**: Index and bloom filter caching for reduced disk I/O
- **XXHash3**: Fast hashing for bloom filters and data integrity
- **Async I/O**: Non-blocking operations for better throughput

### Durability & Recovery
- **Write-Ahead Logging**: All writes logged before memtable modification
- **Crash Recovery**: Automatic recovery from WAL on startup
- **Tombstone Deletions**: Marker-based deletion system for consistency

### Concurrency
- **Background Compaction**: Automatic storage optimization without blocking writes
- **Thread-Safe**: RwLock protection for shared data structures
- **Async/Await**: Modern async patterns with Tokio runtime

## Testing

```bash
# Run all tests
cargo test

# Run with logging
RUST_LOG=info cargo test

# Run specific test
cargo test test_recreating
```

The test suite covers:
- Crash recovery scenarios
- Data consistency guarantees
- Compaction behavior
- CRUD operations

## Requirements

- Rust 2024 edition
- Key dependencies:
  - `tokio` (full features) - Async runtime
  - `serde` - Serialization
  - `xxhash-rust` - Fast hashing
  - `log4rs` - Structured logging
  - `thiserror` - Error handling

## Project Structure

```
src/
├── structures/
│   ├── lsm.rs              # Main LSM-tree implementation
│   ├── memtable.rs         # In-memory table
│   ├── write_ahead_logger.rs  # WAL for durability
│   ├── ss_table_manager.rs # Persistent storage
│   ├── cache.rs            # Index and bloom filter cache
│   ├── compaction_manager.rs # Background compaction
│   ├── manifest.rs         # Metadata tracking
│   └── bloom_filter.rs     # Probabilistic filter
├── config.rs               # Configuration management
├── error.rs                # Error types
└── main.rs                 # CLI interface
```

## CLI Example

```
$ cargo run
application is starting
Enter what to do: add
Enter key: user:1
Enter value: Alice
Enter what to do: add
Enter key: user:2
Enter value: Bob
Enter what to do: get
Enter a key: user:1
"Some(\"Alice\")"
Enter what to do: delete
Enter a key: user:2
Enter what to do: get
Enter a key: user:2
Key is not present!
Enter what to do: exit
```