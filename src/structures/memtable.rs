use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct MemTable {
    pub tree: BTreeMap<String, Option<String>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            tree: BTreeMap::new(),
        }
    }

    pub fn delete(&mut self, key: &str) {
        self.tree.insert(key.to_owned(), None);
    }

    pub fn add(&mut self, key: &str, value: &str) {
        self.tree.insert(key.to_owned(), Some(value.to_owned()));
    }

    pub fn len(&self) -> usize {
        self.tree.len()
    }

    pub fn persist(&mut self) -> Result<(), ()> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(format!(
                "data/sstables/{}.txt",
                MemTable::get_timestamp_ms()
            ))
            .ok()
            .ok_or(())?;

        self.tree.iter().for_each(|e| {
            let value = match e.1 {
                Some(v) => v,
                None => "THOMBSTONE NONE",
            };

            let _ = file.write_all(format!("key {} values {}", e.0, value).as_bytes());
        });

        Ok(())
    }
    fn get_timestamp_ms() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    }
}
