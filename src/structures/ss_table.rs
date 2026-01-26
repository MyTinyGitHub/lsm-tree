use log::info;

use crate::config::Config;
use crate::structures::{cache::Cache, memtable::MemTable};

use std::fs::{self, File, OpenOptions};
use std::io::prelude::*;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct SSTable {}

impl SSTable {
    pub fn read_from_file(file_name: &str, key: &str) -> Option<String> {
        let data =
            fs::read_to_string(file_name).expect(&format!("Error reading from file {}", file_name));

        data.split("\n")
            .skip(1)
            .find(|row| row.split("~").collect::<Vec<&str>>()[0] == key)
            .map(|row| row.split("~").collect::<Vec<&str>>()[1].to_owned())
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

        let _ = file.write_all(mem_table.bloom_filter.persist_value().as_bytes());
        let _ = file.write_all("\n".as_bytes());

        info!("updating cache with a file {} ", file_name);
        mem_table.tree.iter().for_each(|e| {
            let value = match e.1 {
                Some(v) => v,
                None => "TOMBSTONE",
            };

            let _ = file.write_all(format!("{}~{}\n", e.0, value).as_bytes());

            cache
                .write()
                .unwrap()
                .add(&file_name, mem_table.bloom_filter.clone());
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
