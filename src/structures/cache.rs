use core::str;
use std::collections::HashMap;
use std::fs;

use log::info;

use crate::structures::bloom_filter::BloomFilter;

#[derive(Debug, Default)]
pub struct Cache {
    pub bloom_filters: HashMap<String, BloomFilter>,
    pub indexes: HashMap<String, Vec<IndexRecord>>,
}

#[derive(Debug, Default)]
pub struct IndexRecord {
    pub start: String,
    pub end: String,
    pub offset: u64,
    pub size: u64,
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

    pub fn add(
        &mut self,
        file_name: &str,
        bloom_filter: BloomFilter,
        index_vector: Vec<IndexRecord>,
    ) {
        self.bloom_filters
            .insert(file_name.to_string(), bloom_filter);
        self.indexes.insert(file_name.to_string(), index_vector);
    }

    pub fn get(&self, key: &str) -> Vec<&str> {
        self.bloom_filters
            .iter()
            .filter(|(_, value)| value.contains(key))
            .map(|(k, _)| k.as_str())
            .collect::<Vec<&str>>()
    }

    pub fn seek_position(&self, file_name: &str, key: &str) -> Option<(u64, u64)> {
        info!(
            "looking for seek position for filename {} and key {}",
            file_name, key
        );

        let file_indexes = self.indexes.get(file_name)?;
        for index in file_indexes {
            if index.start.as_str() <= key && index.end.as_str() >= key {
                info!("seek location found at {:?}", index);
                return Some((index.offset, index.size));
            }
        }
        info!(
            "seek location not found for filename: {} and key:{}",
            file_name, key
        );
        None
    }
}
