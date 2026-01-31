use std::sync::{Arc, RwLock};

use crate::{
    config::Config,
    error::{LsmError, Result},
    structures::{
        cache::Cache,
        compaction_manager::CompactionManager,
        manifest::Manifest,
        memtable::MemTable,
        ss_table_manager::SSTableManager,
        write_ahead_logger::{self, Operations, WriteAheadLogger},
    },
};

use log::info;

#[derive(Debug)]
pub struct Lsm {
    memtable: Option<MemTable>,
    immutable_memtable: Option<Arc<MemTable>>,
    cache: Arc<RwLock<Cache>>,
    manifest: Arc<RwLock<Manifest>>,
}

impl Default for Lsm {
    fn default() -> Self {
        let memtable = WriteAheadLogger::read_from_file();
        let manifest = Arc::new(RwLock::new(Manifest::new()));
        let cache = Arc::new(RwLock::new(Cache::new()));

        let cache_for_move = Arc::clone(&cache);
        let manifest_for_move = Arc::clone(&manifest);
        tokio::spawn(async move {
            let compaction_manager = CompactionManager::new(manifest_for_move, cache_for_move);
            compaction_manager.monitor().await;
        });

        Self {
            memtable: Some(memtable),
            immutable_memtable: None,
            cache,
            manifest,
        }
    }
}

impl Lsm {
    pub fn add(&mut self, key: &str, value: &str) -> Result<()> {
        info!("Adding an element with key:{} and value:{}", key, value);

        WriteAheadLogger::write(Operations::Put, key, value)
            .ok_or(LsmError::Wal("Failed to write to WAL".to_string()))?;

        if self
            .memtable
            .as_ref()
            .ok_or(LsmError::Wal("No active memtable".to_string()))?
            .len()
            >= Config::global().memtable.max_entries
        {
            self.memtable_to_sstable();

            let mem = Arc::clone(self.immutable_memtable.as_ref().unwrap());
            let cache = Arc::clone(&self.cache);
            let manifest = Arc::clone(&self.manifest);
            tokio::task::spawn_blocking(move || {
                Lsm::persist_immutable_memtable(mem, cache, manifest)
            });
        }

        self.memtable
            .as_mut()
            .ok_or(LsmError::Wal("No active memtable".to_string()))?
            .add(key, value);

        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<String> {
        if let Some(value) = self.memtable.as_ref().and_then(|m: &MemTable| m.get(key)) {
            info!("value found in memtable key: {} value {:?}", key, value);
            return value.clone();
        }

        if let Some(value) = self.immutable_memtable.as_ref().and_then(|m| m.get(key)) {
            info!(
                "value found in immutable_memtable key: {} value {:?}",
                key, value
            );

            return value.clone();
        }

        info!("key {} not found in memtable or immutable_memtable", key);

        let cache = self.cache.read().unwrap();
        let mut files = cache.get(key).clone();

        info!("files found containsing the key {:?}", files);

        files.sort();
        files.reverse();

        files.iter().find_map(|file_name| {
            info!(
                "Value found in cache, retrieve from ss_table file_name: {}",
                file_name
            );

            let seek = cache.seek_position(file_name, key)?;

            SSTableManager::read_from_file(file_name, seek)
                .into_iter()
                .find(|(k, _)| k == key)
                .and_then(|(_, v)| v.clone())
        })
    }

    /*
     * Place a thombstone in the position of the key
     */
    pub fn delete(&mut self, key: &str) -> Result<()> {
        info!("deleting a record with key {}", key);

        WriteAheadLogger::write(Operations::Delete, key, "")
            .ok_or(LsmError::Wal("Failed to write to WAL".to_string()))?;

        self.memtable
            .as_mut()
            .ok_or(LsmError::Wal("No active memtable".to_string()))?
            .delete(key);

        Ok(())
    }

    fn memtable_to_sstable(&mut self) {
        info!("persisting the memtable to file");

        self.immutable_memtable = Some(Arc::new(self.memtable.take().unwrap()));
        self.memtable = Some(MemTable::default());
        write_ahead_logger::increment_index();
    }

    fn persist_immutable_memtable(
        memtable: Arc<MemTable>,
        cache: Arc<RwLock<Cache>>,
        manifest: Arc<RwLock<Manifest>>,
    ) -> Result<()> {
        SSTableManager::persist(memtable, cache, manifest, 0)
            .map_err(|_| LsmError::SsTable("Failed to persist SSTable".to_string()))?;

        Ok(())
    }
}
