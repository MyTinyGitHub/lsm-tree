use std::fs::read;

use crate::structures::{
    cache::Cache,
    memtable::MemTable,
    ss_table::SSTable,
    write_ahead_logger::{Operations, WriteAheadLogger},
};
use log::info;

mod structures;

#[derive(Debug)]
struct Lsm {
    memtable: Option<MemTable>,
    immutable_memtable: Option<MemTable>,
    key_cache: Cache,
    config: Config,
}

#[derive(Debug)]
struct Config {
    max_memtable_size: usize,
    wal_index: usize,
}

impl Lsm {
    fn default() -> Self {
        let wal_index = WriteAheadLogger::list_files_sorted("data/wals")
            .unwrap_or("1".to_owned())
            .parse::<usize>()
            .expect("incorrect and unparsable WAL Index loaded from file");

        Self {
            memtable: Some(MemTable::new()),
            immutable_memtable: None,
            key_cache: Cache::default(),
            config: Config {
                max_memtable_size: 10,
                wal_index,
            },
        }
    }

    fn add(&mut self, key: &str, value: &str) -> Result<(), ()> {
        info!("Adding an element with key:{} and value:{}", key, value);
        WriteAheadLogger::write(
            Operations::Put,
            key,
            value,
            &self.config.wal_index.to_string(),
        );

        if self.memtable.as_ref().ok_or(())?.len() >= self.config.max_memtable_size {
            self.memtable_to_sstable();
        }

        self.memtable.as_mut().ok_or(())?.add(key, value);

        Ok(())
    }

    fn get(&self, key: &str) -> Option<String> {
        if let Some(value) = self.memtable.as_ref().and_then(|v| v.get(key)) {
            info!("value found in memtable key: {} value {:?}", key, value);
            return value.clone();
        }

        if let Some(value) = self.key_cache.get(key) {
            info!(
                "Value found in cache, retrieve from ss_table file_name: {} start_offset: {} end_offset: {}",
                value.file_name, value.offset_start, value.offset_end
            );

            let read_string = SSTable::read_from_file(
                value.file_name.as_ref(),
                value.offset_start,
                value.offset_end,
            );

            return match read_string.as_str() {
                "THOMBSTONE NONE" => None,
                _ => Some(read_string),
            };
        }

        None
    }

    /*
     * Place a thombstone in the position of the key
     */
    fn delete(&mut self, key: &str) -> Result<(), ()> {
        info!("deleting a record with key {}", key);

        WriteAheadLogger::write(
            Operations::Delete,
            key,
            "",
            &self.config.wal_index.to_string(),
        );

        self.memtable.as_mut().ok_or(())?.delete(key);

        Ok(())
    }

    fn memtable_to_sstable(&mut self) {
        info!("persisting the memtable to file");

        self.immutable_memtable = self.memtable.take();
        self.memtable = Some(MemTable::new());
        self.config.wal_index += 1;

        let _ = SSTable::persist(self.immutable_memtable.take().unwrap(), &mut self.key_cache);
    }
}

fn main() {
    log4rs::init_file("./src/config/log4rs.yaml", Default::default()).unwrap();
    info!("application is starting");

    let mut lsm = Lsm::default();
    lsm.memtable = Some(WriteAheadLogger::read_from_file(
        &lsm.config.wal_index.to_string(),
    ));

    info!("after startup {:?}", lsm);

    lsm.add("1", "test").unwrap();
    lsm.add("2", "test").unwrap();
    lsm.add("3", "test").unwrap();
    lsm.add("4", "test").unwrap();
    lsm.add("5", "test").unwrap();
    lsm.add("6", "test").unwrap();
    lsm.add("7", "test").unwrap();
    lsm.add("8", "test").unwrap();
    lsm.add("9", "test").unwrap();
    lsm.delete("9").unwrap();
    lsm.add("10", "test").unwrap();
    lsm.add("11", "test").unwrap();
    lsm.add("22", "test").unwrap();
    lsm.add("33", "test").unwrap();
    lsm.add("44", "test").unwrap();
    lsm.add("55", "test").unwrap();

    let val1 = lsm.get("55");
    info!("Value1 is {:?}", val1);
    let val2 = lsm.get("1");
    info!("Value2 is {:?}", val2);
    let val3 = lsm.get("abc");
    info!("Value3 is {:?}", val3);
    let val4 = lsm.get("9");
    info!("Value4 is {:?}", val4);
}
