# LSM Tree Implementation

A Rust implementation of a Log-Structured Merge Tree (LSM Tree) key-value storage system with write-ahead logging and persistent storage.

## Features

- **Write-Ahead Logging (WAL)**: Ensures data durability by logging all operations before they're applied
- **MemTable Management**: In-memory storage with configurable size limits and automatic flushing
- **SSTable Storage**: Persistent disk storage using Sorted String Tables
- **Key Cache**: Improves read performance with configurable caching
- **Async Operations**: Built on Tokio for concurrent operations
- **Comprehensive Logging**: Structured logging with log4rs for debugging and monitoring

## Architecture

The system consists of several key components:

- **Lsm**: Main tree structure coordinating all operations
- **MemTable**: In-memory sorted data structure for fast writes and reads
- **SSTable**: On-disk sorted immutable tables for persistent storage
- **WriteAheadLogger**: Durability layer ensuring no data loss
- **Cache**: LRU cache for frequently accessed keys

## Usage

### Basic Operations

### Configuration

The system can be configured via `config.toml`:

```toml
[wal]
version = 1

[memtable]
max_entries = 10
```

Or programmatically:

```rust
let config = Config {
    memtable: MemTableConfig { max_entries: 100 },
    directory: Directories {
        wal: "custom/wals/".to_string(),
        ss_table: "custom/ss_tables/".to_string(),
        cache: "custom/cache".to_string(),
        log: "custom/logs.yaml".to_string(),
    },
    wal: WALConfig { version: 1 },
};
```

## Directory Structure

```
data/
├── wals/          # Write-ahead log files
├── ss_tables/     # Sorted String Tables on disk
└── cache          # Key cache storage
log/
└── config/
    └── log4rs.yaml # Logging configuration
```

## Building and Running

```bash
# Build the project
cargo build --release

# Run with default configuration
cargo run

# Run tests
cargo test
```

## Dependencies

- `tokio`: Async runtime
- `log` & `log4rs`: Structured logging
- `md5`: Checksum calculation
- `thiserror`: Error handling

## Error Handling

The system uses a comprehensive error type `LsmError` that covers:

- I/O operations
- Data corruption
- Configuration issues
- Cache operations
- Serialization/deserialization errors

## Performance Considerations

- **Write Performance**: Optimized with WAL and memtable batching
- **Read Performance**: Enhanced with caching and bloom filters
- **Memory Management**: Configurable memtable size balances memory usage and write amplification
- **Disk Usage**: SSTable compaction (when implemented) will reduce storage overhead

