use core::str;
use std::collections::HashMap;
use std::fs;

/**
*
* Written to file as hash | key | offset_start | offset_end | file_name
*/
#[derive(Debug)]
pub struct CacheEntry {
    pub offset_start: u64,
    pub offset_end: u64,
    pub file_name: String,
}

#[derive(Debug)]
pub struct Cache {
    pub data: HashMap<String, CacheEntry>,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            data: HashMap::new(),
        }
    }
}

impl CacheEntry {
    pub fn new(start_pos: u64, end_pos: u64, file_name: &str) -> Self {
        Self {
            offset_start: start_pos,
            offset_end: end_pos,
            file_name: file_name.to_owned(),
        }
    }
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

    pub fn add(&mut self, key: &str, start_pos: u64, end_pos: u64, file_name: &str) {
        self.data.insert(
            key.to_string(),
            CacheEntry::new(start_pos, end_pos, file_name),
        );
    }

    pub fn get(&self, key: &str) -> Option<&CacheEntry> {
        self.data.get(key)
    }
}
