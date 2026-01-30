use log::info;
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::error::LsmError;
use crate::structures::cache::IndexRecord;
use crate::structures::manifest::{Manifest, SSTableBasicInfo};
use crate::structures::{cache::Cache, memtable::MemTable};

use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::sync::{Arc, RwLock};

#[derive(Debug)]
pub struct SSTableManager {}

#[derive(Serialize, Deserialize)]
pub struct SSTableFooter {
    pub bloom_filter_offset: u64,
    pub bloom_filter_size: u64,
    pub index_offset: u64,
    pub index_size: u64,
}

impl SSTableManager {
    pub fn read_from_file(
        file_name: &str,
        index_record: &IndexRecord,
    ) -> Vec<(String, Option<String>)> {
        info!(
            "Reading from file {} offset {} and size {}",
            file_name, index_record.offset, index_record.size
        );

        let mut file = File::open(file_name).expect("Can't open file");

        let _ = file.seek(std::io::SeekFrom::Start(index_record.offset));
        let mut buffer = vec![0u8; index_record.size as usize];

        let _ = file.read_exact(&mut buffer);

        bincode::deserialize(&buffer).unwrap()
    }

    pub fn persist(
        mem_table: Arc<MemTable>,
        cache: Arc<RwLock<Cache>>,
        manifest: Arc<RwLock<Manifest>>,
        level: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (file_name, id) = manifest.write().unwrap().create_filename(level);

        info!("writing to a file {}", file_name);

        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&file_name)
            .ok()
            .ok_or(LsmError::SsTable("Unable to open file".to_owned()))?;

        info!("updating cache with a file {} ", file_name);

        let mut indexes: Vec<IndexRecord> = Vec::new();
        let mut index_offset = 0;
        let mut index_key: &str = "";
        let mut index_end_key: &str = "";
        let mut vect: Vec<(String, Option<String>)> = Vec::new();

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
            vect.push((key.clone(), value.clone()));
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

        manifest
            .write()
            .expect("unable to open manifest for writes")
            .add(SSTableBasicInfo::new(
                id,
                file_name.to_string(),
                level,
                "".to_string(),
                "".to_string(),
            ));
        Ok(())
    }
}
