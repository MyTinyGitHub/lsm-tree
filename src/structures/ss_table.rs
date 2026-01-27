use log::info;
use log4rs::append::file;

use crate::config::Config;
use crate::structures::cache::IndexRecord;
use crate::structures::{cache::Cache, memtable::MemTable};

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::prelude::*;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct SSTable {}

pub struct SSTableFooter {
    bloom_filter_offset: u64,
    bloom_filter_data: u64,
    index_offset: u64,
    index_size: u64,
    version: u32,
    checksum: u32,
    magic_number: u64,
}

impl SSTable {
    pub fn read_from_file(
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

        let data = String::from_utf8(buffer).unwrap();

        data.split("\n").find_map(|row| {
            let split_row = row.split("~").collect::<Vec<&str>>();
            if split_row[0] != key {
                return None;
            }

            Some(split_row[1].to_owned())
        })
    }

    pub fn persist(mem_table: Arc<MemTable>, cache: Arc<RwLock<Cache>>) -> Result<(), ()> {
        let file_name = format!(
            "{}/{}.txt",
            Config::global().directory.ss_table,
            SSTable::get_timestamp_ms()
        );

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

        for (index, (key, value)) in mem_table.tree.iter().enumerate() {
            if index != 0 && index % 5 == 0 {
                indexes.push(IndexRecord {
                    start: index_key.to_owned(),
                    end: index_end_key.to_owned(),
                    offset: index_offset,
                    size: file.stream_position().unwrap() - index_offset,
                });
            }

            if index % 5 == 0 {
                index_offset = file.stream_position().unwrap();
                index_key = key;
            }

            let value = value.as_deref().unwrap_or("TOMBSTONE");
            let key_value = format!("{}~{}\n", key, value);
            let _ = file.write_all(key_value.as_bytes());

            index_end_key = key;
        }

        indexes.push(IndexRecord {
            start: index_key.to_owned(),
            end: index_end_key.to_owned(),
            offset: index_offset,
            size: file.stream_position().unwrap() - index_offset,
        });

        info!("indexes to write {:?}", indexes);

        let _ = file.write_all("\n".as_bytes());

        let bloom_filter = mem_table.bloom_filter.persist_value() + "\n";
        let bloom_filter_offset = file.stream_position().unwrap();
        let bloom_filter_size = bloom_filter.len() as u64;

        let _ = file.write_all(bloom_filter.as_bytes());

        let index_offset = file.stream_position().unwrap();

        let mut index_formatted = "".to_owned();
        for index_record in indexes.iter() {
            index_formatted.push_str(&format!(
                "{}~{}~{}~{}\n",
                &index_record.start, &index_record.end, index_record.offset, index_record.size
            ));
        }

        let index_size = index_formatted.len() as u64;
        let _ = file.write_all(index_formatted.as_bytes());

        let _ = file.write_all(&bloom_filter_offset.to_le_bytes());
        let _ = file.write_all(&bloom_filter_size.to_le_bytes());
        let _ = file.write_all(&index_offset.to_le_bytes());
        let _ = file.write_all(&index_size.to_le_bytes());

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
