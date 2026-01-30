use log::info;
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::structures::cache::IndexRecord;
use crate::structures::manifest::Manifest;
use crate::structures::{cache::Cache, memtable::MemTable};

use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct SSTableManager {
    manifest: Arc<RwLock<Manifest>>,
}

#[derive(Serialize, Deserialize)]
pub struct SSTableFooter {
    pub bloom_filter_offset: u64,
    pub bloom_filter_size: u64,
    pub index_offset: u64,
    pub index_size: u64,
}

impl SSTableManager {
    pub fn new(manifest: Arc<RwLock<Manifest>>) -> Self {
        Self { manifest }
    }

    pub fn read_from_file(
        &self,
        file_name: &str,
        key: &str,
        (offset, size): (u64, u64),
    ) -> Option<String> {
        info!(
            "Reading from file {} offset {} and size {}",
            file_name, offset, size
        );

        let mut file = File::open(file_name).expect("Can't open file");

        let _ = file.seek(std::io::SeekFrom::Start(offset));
        let mut buffer = vec![0u8; size as usize];

        let _ = file.read_exact(&mut buffer);

        let values: Vec<(&str, Option<&str>)> = bincode::deserialize(&buffer).unwrap();
        values.iter().find_map(|(k, v)| {
            if *k == key {
                return v.map(|v2| v2.to_owned());
            }
            None
        })
    }

    pub fn persist(&self, mem_table: Arc<MemTable>, cache: Arc<RwLock<Cache>>) -> Result<(), ()> {
        let file_name = self.manifest.write().unwrap().create_level_0_filename();

        info!("writing to a file {}", file_name);

        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&file_name)
            .ok()
            .ok_or(())?;

        info!("updating cache with a file {} ", file_name);

        let mut indexes: Vec<IndexRecord> = Vec::new();
        let mut index_offset = 0;
        let mut index_key: &str = "";
        let mut index_end_key: &str = "";
        let mut vect: Vec<(&str, Option<&str>)> = Vec::new();

        for (index, (key, value)) in mem_table.tree.iter().enumerate() {
            if index != 0 && index % Config::global().cache.index_size == 0 {
                info!("Persisting vect:{:?}", vect);

                let bytes = bincode::serialize(&vect).unwrap();
                let _ = file.write_all(&bytes);

                indexes.push(IndexRecord {
                    start: index_key.to_owned(),
                    end: index_end_key.to_owned(),
                    offset: index_offset,
                    size: bytes.len() as u64,
                });

                vect = Vec::new();
            }

            if index % Config::global().cache.index_size == 0 {
                index_offset = file.stream_position().unwrap();
                index_key = key;
            }

            index_end_key = key;
            vect.push((key, value.as_deref()));
        }

        info!("Persisted vect:{:?}", vect);

        let bytes = bincode::serialize(&vect).unwrap();
        let _ = file.write_all(&bytes);

        indexes.push(IndexRecord {
            start: index_key.to_owned(),
            end: index_end_key.to_owned(),
            offset: index_offset,
            size: bytes.len() as u64,
        });

        info!("indexes to write {:?}", indexes);

        let bloom_filter = bincode::serialize(&mem_table.bloom_filter).unwrap();
        let bloom_filter_offset = file.stream_position().unwrap();
        let bloom_filter_size = bloom_filter.len() as u64;

        let _ = file.write_all(&bloom_filter);

        let index_offset = file.stream_position().unwrap();

        let index_bytes = bincode::serialize(&indexes).unwrap();
        let _ = file.write_all(&index_bytes);

        let footer = SSTableFooter {
            bloom_filter_offset,
            bloom_filter_size,
            index_offset,
            index_size: index_bytes.len() as u64,
        };

        let footer_bytes = bincode::serialize(&footer).unwrap();
        let _ = file.write_all(&footer_bytes);

        cache
            .write()
            .unwrap()
            .add(&file_name, mem_table.bloom_filter.clone(), indexes);

        Ok(())
    }

    fn get_timestamp_ms() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    }
}
