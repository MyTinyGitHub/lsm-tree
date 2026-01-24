use log::info;

use crate::config::Config;
use crate::structures::{cache::Cache, memtable::MemTable};

use std::fs::{File, OpenOptions};
use std::io::SeekFrom;
use std::io::prelude::*;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct SSTable {}

impl SSTable {
    pub fn read_from_file(file_name: &str, start_pos: u64, end_pos: u64) -> String {
        let mut file = File::open(file_name).unwrap();
        let length = end_pos - start_pos;
        let mut buffer = vec![0u8; length as usize];

        file.seek(SeekFrom::Start(start_pos)).unwrap();
        file.read_exact(&mut buffer).unwrap();

        let value = String::from_utf8(buffer).unwrap();

        let split = value.split("~").last();

        split.unwrap().strip_suffix("\n").unwrap().to_owned()
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
        mem_table.tree.iter().for_each(|e| {
            let value = match e.1 {
                Some(v) => v,
                None => "TOMBSTONE",
            };

            let start_position = file.stream_position().unwrap();
            let _ = file.write_all(format!("{}~{}\n", e.0, value).as_bytes());
            let end_position = file.stream_position().unwrap();

            cache
                .write()
                .unwrap()
                .add(e.0, start_position, end_position, &file_name);
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
