use crate::structures::{cache::Cache, memtable::MemTable};

use std::fs::{File, OpenOptions};
use std::io::SeekFrom;
use std::io::prelude::*;
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
        return split.unwrap().to_owned();
    }

    pub fn persist(mem_table: MemTable, cache: &mut Cache) -> Result<(), ()> {
        let file_name = format!("data/sstables/{}.txt", SSTable::get_timestamp_ms());

        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&file_name)
            .ok()
            .ok_or(())?;

        mem_table.tree.iter().for_each(|e| {
            let value = match e.1 {
                Some(v) => v,
                None => "THOMBSTONE NONE",
            };

            let start_position = file.stream_position().unwrap();
            let _ = file.write_all(format!("{}~{}", e.0, value).as_bytes());
            let end_position = file.stream_position().unwrap();

            cache.add(e.0, start_position, end_position, &file_name);
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
