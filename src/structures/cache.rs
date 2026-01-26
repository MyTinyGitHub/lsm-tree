use core::str;
use std::collections::HashMap;
use std::fs;

use crate::structures::bloom_filter::BloomFilter;

#[derive(Debug, Default)]
pub struct Cache {
    pub data: HashMap<String, BloomFilter>,
}

impl Cache {
    pub fn read_on_startup(&mut self, path: String) {
        let mut entries = fs::read_dir(path)
            .unwrap()
            .filter(|d| d.as_ref().unwrap().file_name() != ".gitkeep")
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        entries.sort_by_key(|e| e.file_name());
    }

    pub fn add(&mut self, file_name: &str, bloom_filter: BloomFilter) {
        self.data.insert(file_name.to_string(), bloom_filter);
    }

    pub fn get(&self, key: &str) -> Vec<&str> {
        self.data
            .iter()
            .filter(|(_, value)| value.contains(key))
            .map(|(k, _)| k.as_str())
            .collect::<Vec<&str>>()
    }
}
