use thiserror::Error;

#[derive(Error, Debug)]
pub enum LsmError {
    #[error("WAL operation failed: {0}")]
    WalError(String),

    #[error("SSTable operation failed: {0}")]
    SsTableError(String),

    #[error("Cache operation failed: {0}")]
    CacheError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Logging error: {0}")]
    LogError(String),

    #[error("Key not found: {0}")]
    KeyNotFound(String),
}

pub type Result<T> = std::result::Result<T, LsmError>;
