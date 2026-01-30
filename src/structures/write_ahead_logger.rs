use crate::config::Config;
use crate::structures::memtable::MemTable;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct WriteAheadLogger {}

#[derive(Debug, Serialize, Deserialize)]
pub enum Operations {
    Put,
    Delete,
}

#[derive(Serialize, Deserialize)]
struct WALRecord {
    version: usize,
    checksum: String,
    operation: Operations,
    key: String,
    value: String,
}

static WAL_INDEX: AtomicUsize = AtomicUsize::new(0);

fn init_index() -> usize {
    let file_name: String = WriteAheadLogger::list_latest().unwrap_or_else(|_| "1".to_owned());

    WAL_INDEX.store(
        file_name
            .parse::<usize>()
            .expect("unable to parse initial WAL Index"),
        Ordering::SeqCst,
    );

    WAL_INDEX.load(Ordering::SeqCst)
}

pub fn increment_index() -> usize {
    WAL_INDEX.fetch_add(1, Ordering::SeqCst)
}

pub fn index() -> usize {
    WAL_INDEX.load(Ordering::SeqCst)
}

fn wal_file_path() -> String {
    format!("{}/{:010}.txt", Config::global().directory.wal, index())
}

fn operation_value(operation: &Operations) -> String {
    match operation {
        Operations::Put => "PUT".to_owned(),
        Operations::Delete => "DELETE".to_owned(),
    }
}

impl FromStr for Operations {
    type Err = ();

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "PUT" => Ok(Operations::Put),
            "DELETE" => Ok(Operations::Delete),
            _ => Err(()),
        }
    }
}

impl WriteAheadLogger {
    pub fn list_latest() -> Result<String, ()> {
        let file_path = &Config::global().directory.wal;

        let mut entries = fs::read_dir(file_path)
            .map_err(|_| ())?
            .filter(|d| d.as_ref().unwrap().file_name() != ".gitkeep")
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        entries.sort_by_key(|e| e.file_name());

        let entry = entries.last().ok_or(())?;

        info!("{:?}", entry.file_name());

        Ok(entry
            .path()
            .file_stem()
            .ok_or(())?
            .to_string_lossy()
            .to_string())
    }

    pub fn read_from_file() -> MemTable {
        init_index();
        let Ok(data) = fs::read_to_string(wal_file_path()) else {
            return MemTable::default();
        };

        let mut tree = MemTable::default();

        data.split("|").take_while(|v| !v.is_empty()).for_each(|v| {
            let wal_record: WALRecord = bincode::deserialize(v.as_bytes()).unwrap();

            let calc_checksum = md5::compute(format!("{}{}", wal_record.key, wal_record.value));

            if format!("{:x}", calc_checksum) != wal_record.checksum {
                error!("Corrupted WAL Record mismatched checksum");
                return;
            }

            match wal_record.operation {
                Operations::Put => {
                    tree.add(&wal_record.key, &wal_record.value);
                }
                Operations::Delete => {
                    tree.delete(&wal_record.key);
                }
            };
        });

        tree
    }

    pub fn write(operation: Operations, key: &str, value: &str) -> Option<bool> {
        info!(
            "writing to wal operation {:?} key {} value {}",
            operation, key, value
        );

        let file_name = wal_file_path();

        info!("reading file {}", &file_name);

        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&file_name)
            .ok()?;

        info!("file {} openned", &file_name);

        let checksum = md5::compute(format!("{}{}", key, value));

        let wal_record = WALRecord {
            version: Config::global().wal.version,
            checksum: format!("{:x}", checksum),
            operation,
            key: key.to_string(),
            value: value.to_string(),
        };

        let bytes = bincode::serialize(&wal_record).expect("Unable to serialize WALRecord");
        file.write_all(&bytes).ok()?;
        file.write_all(b"|").ok()?;

        Some(true)
    }
}
