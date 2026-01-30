use core::str;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};

use log::info;

use crate::config::Config;
use crate::structures::bloom_filter::BloomFilter;

#[derive(Debug, Default)]
pub struct Cache {
    pub bloom_filters: BTreeMap<String, BloomFilter>,
    pub indexes: BTreeMap<String, Vec<IndexRecord>>,
}

#[derive(Debug, Default)]
pub struct IndexRecord {
    pub start: String,
    pub end: String,
    pub offset: u64,
    pub size: u64,
}

impl IndexRecord {
    fn from_string(input: &str) -> Self {
        let parts = input.split("~").collect::<Vec<_>>();
        info!("{:?}", parts);
        Self {
            start: parts[0].to_owned(),
            end: parts[1].to_owned(),
            offset: parts[2].parse().expect("Unable to parse offset"),
            size: parts[3].parse().expect("Unable to parse size"),
        }
    }
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

            let bloom_filter_offset = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
            let bloom_filter_size = u64::from_le_bytes(buffer[8..16].try_into().unwrap());
            let index_offset = u64::from_le_bytes(buffer[16..24].try_into().unwrap());
            let index_size = u64::from_le_bytes(buffer[24..32].try_into().unwrap());

            self.read_indexes_from_file(
                &mut opened,
                file.path().to_str().unwrap().to_string(),
                index_offset,
                index_size,
            );

            self.read_bloom_filter_from_file(
                &mut opened,
                file.path().to_str().unwrap().to_string(),
                bloom_filter_offset,
                bloom_filter_size,
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

        let mut index_vector: Vec<IndexRecord> = Vec::new();

        String::from_utf8(buffer)
            .expect("Unable to parse index")
            .strip_suffix("\n")
            .expect("Unable to strip suffix")
            .split("\n")
            .for_each(|v| {
                let index_record = IndexRecord::from_string(v);
                index_vector.push(index_record);
            });

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

        let bloom_filter = String::from_utf8(buffer).expect("Unable to parse bloom filter");
        self.bloom_filters
            .insert(file_name, BloomFilter::from_string(&bloom_filter));
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
