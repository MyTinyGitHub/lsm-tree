use crate::structures::{
    cache::Cache,
    memtable::MemTable,
    ss_table::SSTable,
    write_ahead_logger::{Operations, WriteAheadLogger},
};

use log::info;

#[derive(Debug)]
struct Config {
    max_memtable_size: usize,
    wal_index: usize,
}

#[derive(Debug)]
pub struct Lsm {
    memtable: Option<MemTable>,
    immutable_memtable: Option<MemTable>,
    key_cache: Cache,
    config: Config,
}

impl Lsm {
    pub fn default() -> Self {
        let wal_index = WriteAheadLogger::list_files_sorted("data/wals")
            .unwrap_or("1".to_owned())
            .parse::<usize>()
            .expect("incorrect and unparsable WAL Index loaded from file");

        let memtable = WriteAheadLogger::read_from_file(&wal_index.to_string());

        Self {
            memtable: Some(memtable),
            immutable_memtable: None,
            key_cache: Cache::default(),
            config: Config {
                max_memtable_size: 10,
                wal_index,
            },
        }
    }

    pub fn add(&mut self, key: &str, value: &str) -> Result<(), ()> {
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

    pub fn get(&self, key: &str) -> Option<String> {
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
                "THOMBSTONE" => None,
                _ => Some(read_string),
            };
        }

        None
    }

    /*
     * Place a thombstone in the position of the key
     */
    pub fn delete(&mut self, key: &str) -> Result<(), ()> {
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

    pub fn memtable_to_sstable(&mut self) {
        info!("persisting the memtable to file");

        self.immutable_memtable = self.memtable.take();
        self.memtable = Some(MemTable::new());
        self.config.wal_index += 1;

        let _ = SSTable::persist(self.immutable_memtable.take().unwrap(), &mut self.key_cache);
    }
}
