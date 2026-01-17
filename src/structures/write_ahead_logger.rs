use crate::structures::memtable::MemTable;
use log::{error, info};
use std::fs;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::str::FromStr;

const WAL_VERSION: u8 = 1;

#[derive(Debug)]
pub struct WriteAheadLogger {}

#[derive(Debug)]
pub enum Operations {
    Put,
    Delete,
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
    pub fn list_files_sorted(path: &str) -> Result<String, ()> {
        let mut entries = fs::read_dir(path)
            .unwrap()
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

    pub fn read_from_file(path: &str) -> MemTable {
        let Ok(data) = fs::read_to_string(format!("data/wals/{}.txt", path)) else {
            return MemTable::new();
        };

        let mut tree = MemTable::new();

        data.split("\n")
            .take_while(|v| !v.is_empty())
            .for_each(|v| {
                let split = v.split("~").collect::<Vec<&str>>();

                let _ = split[0];
                let checksum = split[1];
                let Ok(operation) = Operations::from_str(split[2]) else {
                    error!("Corrupted WAL Unable to read operation");
                    return;
                };
                let key = split[3];
                let value = split[4];

                let calc_checksum = md5::compute(format!("{}~{}~", key, value));

                if format!("{:x}", calc_checksum) != checksum {
                    error!("Corrupted WAL Record mismatched checksum");
                    return;
                }

                match operation {
                    Operations::Put => {
                        tree.add(split[3], split[4]);
                    }
                    Operations::Delete => {
                        tree.delete(split[3]);
                    }
                };
            });

        tree
    }

    pub fn write(operation: Operations, key: &str, value: &str, file: &str) -> Option<bool> {
        info!(
            "writing to wal operation {:?} key {} value {}",
            operation, key, value
        );

        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(format!("data/wals/{}.txt", file))
            .ok()?;

        let version_formatted = format!("v{}~", WAL_VERSION);
        let operation_formatted = format!("{}~", operation_value(&operation));
        let formatted_key = format!("{}~", key);

        let formatted_value = match &operation {
            Operations::Put => format!("{}~", value),
            Operations::Delete => "~".to_owned(),
        };

        let checksum = md5::compute(format!("{}{}", formatted_key, formatted_value));

        file.write_all(version_formatted.as_bytes()).ok()?;
        file.write_all(format!("{:x}", checksum).as_bytes()).ok()?;
        file.write_all(b"~").ok()?;
        file.write_all(operation_formatted.as_ref()).ok()?;
        file.write_all(formatted_key.as_bytes()).ok()?;
        file.write_all(formatted_value.as_bytes()).ok()?;
        file.write_all("\n".as_bytes()).ok()?;

        Some(true)
    }
}
