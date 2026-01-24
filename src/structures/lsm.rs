use std::sync::{Arc, RwLock};

use crate::{
    config::Config,
    error::{LsmError, Result},
    structures::{
        cache::Cache,
        memtable::MemTable,
        ss_table::SSTable,
        write_ahead_logger::{Operations, WriteAheadLogger},
    },
};

use log::info;

#[derive(Debug)]
pub struct Lsm {
    memtable: Option<MemTable>,
    immutable_memtable: Option<Arc<MemTable>>,
    key_cache: Arc<RwLock<Cache>>,
    wal_index: usize,
    wal_storage_location: String,
    max_memtable_size: usize,
}

impl Lsm {
    pub fn new(config: Config) -> Self {
        let wal_index = WriteAheadLogger::list_files_sorted("data/wals")
            .unwrap_or("1".to_owned())
            .parse::<usize>()
            .expect("incorrect and unparsable WAL Index loaded from file");

        let memtable = WriteAheadLogger::read_from_file(&wal_index.to_string());

        Self {
            memtable: Some(memtable),
            immutable_memtable: None,
            key_cache: Arc::new(RwLock::new(Cache::default())),
            wal_index,
            max_memtable_size: config.memtable.max_entries,
            wal_storage_location: config.directory.wal,
        }
    }

    fn wal_file_path(&self) -> String {
        format!("{}{}.txt", self.wal_storage_location, self.wal_index)
    }

    pub fn add(&mut self, key: &str, value: &str) -> Result<()> {
        info!("Adding an element with key:{} and value:{}", key, value);

        WriteAheadLogger::write(Operations::Put, key, value, &self.wal_file_path())
            .ok_or(LsmError::WalError("Failed to write to WAL".to_string()))?;

        if self
            .memtable
            .as_ref()
            .ok_or(LsmError::WalError("No active memtable".to_string()))?
            .len()
            >= self.max_memtable_size
        {
            self.memtable_to_sstable();

            let mem = Arc::clone(self.immutable_memtable.as_ref().unwrap());
            let cache = Arc::clone(&self.key_cache);
            tokio::task::spawn_blocking(move || Lsm::persist_immutable_memtable(mem, cache));
        }

        self.memtable
            .as_mut()
            .ok_or(LsmError::WalError("No active memtable".to_string()))?
            .add(key, value);

        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<String> {
        if let Some(value) = self.memtable.as_ref().and_then(|v| v.get(key)) {
            info!("value found in memtable key: {} value {:?}", key, value);
            return value.clone();
        }

        if let Some(value) = self.immutable_memtable.as_ref().and_then(|t| t.get(key)) {
            info!(
                "value found in immutable_memtable key: {} value {:?}",
                key, value
            );
            return value.clone();
        }

        if let Some(value) = self.key_cache.read().unwrap().get(key) {
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
                "TOMBSTONE" => None,
                _ => Some(read_string),
            };
        }

        None
    }

    /*
     * Place a thombstone in the position of the key
     */
    pub fn delete(&mut self, key: &str) -> Result<()> {
        info!("deleting a record with key {}", key);

        WriteAheadLogger::write(Operations::Delete, key, "", &self.wal_file_path())
            .ok_or(LsmError::WalError("Failed to write to WAL".to_string()))?;

        self.memtable
            .as_mut()
            .ok_or(LsmError::WalError("No active memtable".to_string()))?
            .delete(key);

        Ok(())
    }

    fn memtable_to_sstable(&mut self) {
        info!("persisting the memtable to file");

        self.immutable_memtable = Some(Arc::new(self.memtable.take().unwrap()));
        self.memtable = Some(MemTable::new());
        self.wal_index += 1;
    }

    fn persist_immutable_memtable(
        memtable: Arc<MemTable>,
        cache: Arc<RwLock<Cache>>,
    ) -> Result<()> {
        let _ = SSTable::persist(memtable, cache)
            .map_err(|_| LsmError::SsTableError("Failed to persist SSTable".to_string()))?;
        Ok(())
    }
}
