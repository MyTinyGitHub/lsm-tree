use thiserror::Error;

#[derive(Error, Debug)]
pub enum LsmError {
    #[error("WAL operation failed: {0}")]
    Wal(String),

    #[error("SSTable operation failed: {0}")]
    SsTable(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Logging error: {0}")]
    Log(String),
}

pub type Result<T> = std::result::Result<T, LsmError>;
