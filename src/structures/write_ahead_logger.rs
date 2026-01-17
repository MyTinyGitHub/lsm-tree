use crate::structures::memtable::MemTable;
use log::info;
use std::fs::OpenOptions;
use std::fs::{self};
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
        Operations::Delete => "PUT".to_owned(),
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
            info!("Data inside a wal could not be read, defeulting to empty MemTable");
            return MemTable::new();
        };

        let mut tree = MemTable::new();

        data.split("\n")
            .take_while(|v| !v.is_empty())
            .for_each(|v| {
                let split = v.split("|").collect::<Vec<&str>>();

                match Operations::from_str(split[1]).unwrap() {
                    Operations::Put => {
                        tree.add(split[2], split[3]);
                    }
                    Operations::Delete => {
                        tree.delete(split[2]);
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

        let version_formatted = format!("v{}|", WAL_VERSION);
        let operation_formatted = format!("{}|", operation_value(&operation));
        let formatted_key = format!("{}|", key);

        let formatted_value = match &operation {
            Operations::Put => format!("{}|", value),
            Operations::Delete => "".to_owned(),
        };

        file.write_all(version_formatted.as_bytes()).ok()?;
        file.write_all(operation_formatted.as_bytes()).ok()?;
        file.write_all(formatted_key.as_bytes()).ok()?;
        file.write_all(formatted_value.as_bytes()).ok()?;
        file.write_all("\n".as_bytes()).ok()?;

        Some(true)
    }
}
