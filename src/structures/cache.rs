use core::str;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};

use log::info;
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::structures::bloom_filter::BloomFilter;
use crate::structures::ss_table::SSTableFooter;

#[derive(Debug, Default)]
pub struct Cache {
    pub bloom_filters: BTreeMap<String, BloomFilter>,
    pub indexes: BTreeMap<String, Vec<IndexRecord>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct IndexRecord {
    pub start: String,
    pub end: String,
    pub offset: u64,
    pub size: u64,
}

impl Cache {
    pub fn new() -> Self {
        let mut res = Self::default();
        res.read_on_startup();
        res
    }

    pub fn read_on_startup(&mut self) {
        let file_path = Config::global().directory.ss_table.as_str();

        let mut entries = fs::read_dir(file_path)
            .unwrap()
            .filter(|d| d.as_ref().unwrap().file_name() != ".gitkeep")
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        entries.sort_by_key(|e| e.file_name());

        let seek = SeekFrom::End(-32);
        for file in entries {
            info!("Processing file: {:?}", file.path());

            let mut opened = File::open(file.path()).expect("Unable to open file");
            opened.seek(seek).expect("Unable to seek to the position");

            let mut buffer = vec![0u8; 32];
            opened
                .read_exact(&mut buffer)
                .expect("Unable to read the file");

            let footer: SSTableFooter = bincode::deserialize(&buffer).unwrap();

            self.read_indexes_from_file(
                &mut opened,
                file.path().to_str().unwrap().to_string(),
                footer.index_offset,
                footer.index_size,
            );

            self.read_bloom_filter_from_file(
                &mut opened,
                file.path().to_str().unwrap().to_string(),
                footer.bloom_filter_offset,
                footer.bloom_filter_size,
            );
        }
    }

    fn read_indexes_from_file(
        &mut self,
        file: &mut File,
        file_name: String,
        offset: u64,
        size: u64,
    ) {
        let seek = SeekFrom::Start(offset);
        file.seek(seek).expect("Unable to seek to the position");

        let mut buffer = vec![0u8; size as usize];
        file.read_exact(&mut buffer)
            .expect("Unable to read the file");

        let index_vector: Vec<IndexRecord> = bincode::deserialize(&buffer).unwrap();

        info!("read index vector: {:?}", index_vector);

        self.indexes.insert(file_name, index_vector);
    }

    fn read_bloom_filter_from_file(
        &mut self,
        file: &mut File,
        file_name: String,
        offset: u64,
        size: u64,
    ) {
        let seek = SeekFrom::Start(offset);
        file.seek(seek).expect("Unable to seek to the position");

        let mut buffer = vec![0u8; size as usize];
        file.read_exact(&mut buffer)
            .expect("Unable to read the file");

        info!("bloomfilter offset {} size {}", offset, size);

        let bloom_filter: BloomFilter = bincode::deserialize(&buffer).unwrap();
        self.bloom_filters.insert(file_name, bloom_filter);
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
