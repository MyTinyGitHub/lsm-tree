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
struct CacheConfig {
    file_name: String,
}

#[derive(Debug)]
pub struct Cache {
    pub data: HashMap<String, CacheEntry>,
    config: CacheConfig,
}

impl CacheEntry {
    pub fn new(offset_start: u64, offset_end: u64, file_name: &str) -> Self {
        Self {
            offset_start,
            offset_end,
            file_name: file_name.to_owned(),
        }
    }
}

impl Default for Cache {
    fn default() -> Self {
        let mut cache = Self {
            data: HashMap::new(),
            config: CacheConfig {
                file_name: "/data/cache/entries.txt".to_owned(),
            },
        };

        cache.read_from_file();
        cache
    }
}

impl Cache {
    pub fn read_from_file(&mut self) {
        let data = fs::read_to_string(&self.config.file_name);

        if data.is_err() {
            return;
        }

        data.unwrap()
            .split("\n")
            .take_while(|v| !v.is_empty())
            .for_each(|v| {
                let split = v.split("|").collect::<Vec<&str>>();

                let Ok(offset_start) = split[2].parse::<u64>() else {
                    log::error!("Unable to parse values {:?}", split);
                    return;
                };

                let Ok(offset_end) = split[3].parse::<u64>() else {
                    log::error!("Unable to parse values {:?}", split);
                    return;
                };

                let entry = CacheEntry::new(offset_start, offset_end, split[4]);

                self.data.insert(split[1].to_owned(), entry);
            });
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
