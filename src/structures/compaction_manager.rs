use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    sync::{Arc, RwLock},
    time::Duration,
};

use log::trace;

use crate::{
    config::Config,
    structures::{
        cache::{self, Cache, IndexRecord},
        manifest::Manifest,
        memtable::MemTable,
        ss_table_manager::{SSTableFooter, SSTableManager},
    },
};

#[derive(Debug)]
pub struct CompactionManager {
    manifest: Arc<RwLock<Manifest>>,
    cache: Arc<RwLock<Cache>>,
}

impl CompactionManager {
    pub fn new(manifest: Arc<RwLock<Manifest>>, cache: Arc<RwLock<Cache>>) -> Self {
        Self { manifest, cache }
    }

    pub async fn monitor(&self) {
        loop {
            self.monitor_l0();
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    }

    pub fn monitor_l0(&self) {
        let ss_table_count = self
            .manifest
            .read()
            .expect("Unable to aquire read lock")
            .ss_tables_in_level(0)
            .len();

        if ss_table_count >= Config::global().ss_table.l0_file_count_limit {
            self.compact(0);
        } else {
            trace!("nothing to do for compaction")
        }
    }

    pub fn compact(&self, level: usize) {
        trace!("starting compaction for level {}", level);
        trace!("giong to compact {:?}", self.manifest);

        let ss_tables = self
            .manifest
            .write()
            .expect("Unable to aquire read lock")
            .compaction_nominees(0);

        trace!("compaction nominees are {:?} ", ss_tables);

        let path1 = ss_tables[0].path.as_str();
        let values1 = self
            .read_file_into_index_record(path1)
            .iter()
            .flat_map(|i| SSTableManager::read_from_file(path1, i))
            .collect::<Vec<(String, Option<String>)>>();

        let path2 = ss_tables[1].path.as_str();
        let values2 = self
            .read_file_into_index_record(path2)
            .iter()
            .flat_map(|i| SSTableManager::read_from_file(path2, i))
            .collect::<Vec<(String, Option<String>)>>();

        trace!("read value vector: {:?}", values1);
        trace!("read value vector: {:?}", values2);

        let mut index1 = 0;
        let mut index2 = 0;

        let mut result = MemTable::default();

        loop {
            if index1 >= values1.len() && index2 >= values2.len() {
                break;
            }

            if index1 >= values1.len() {
                self.add_to_mem_table(&mut result, &values2[index2]);
                index2 += 1;
                continue;
            }

            if index2 >= values2.len() {
                self.add_to_mem_table(&mut result, &values1[index1]);
                index1 += 1;
                continue;
            }

            let key1 = &values1[index1].0;
            let key2 = &values2[index1].0;

            if key1 <= key2 {
                self.add_to_mem_table(&mut result, &values1[index1]);
                index1 += 1;
            } else {
                self.add_to_mem_table(&mut result, &values2[index2]);
                index2 += 1;
            }
        }

        trace!("final memtable after compaction {:?}", result);

        SSTableManager::persist(
            Arc::new(result),
            Arc::clone(&self.cache),
            Arc::clone(&self.manifest),
            level + 1,
        )
        .expect("Unable to persist compacted MemTable");

        self.manifest
            .write()
            .expect("Unable to get lock on manifest")
            .remove(&ss_tables[0]);

        self.manifest
            .write()
            .expect("Unable to get lock on manifest")
            .remove(&ss_tables[1]);

        self.cache
            .write()
            .expect("Unable to get lock to cache")
            .delete(path1);

        self.cache
            .write()
            .expect("Unable to get lock to cache")
            .delete(path2);

        let _ = fs::remove_file(path1);
        let _ = fs::remove_file(path2);
    }

    fn add_to_mem_table(&self, res: &mut MemTable, (key, value): &(String, Option<String>)) {
        match value {
            Some(value) => res.add(key, value),
            None => res.delete(key),
        }
    }

    fn read_file_into_index_record(&self, file_path: &str) -> Vec<IndexRecord> {
        trace!("Processing file: {:?}", file_path);

        let seek = SeekFrom::End(-32);

        let mut file = File::open(file_path).expect("Unable to open file");
        file.seek(seek).expect("Unable to seek to the position");

        let mut buffer = vec![0u8; 32];
        file.read_exact(&mut buffer)
            .expect("Unable to read the file");

        let footer: SSTableFooter = bincode::deserialize(&buffer).unwrap();

        let seek = SeekFrom::Start(footer.index_offset);
        file.seek(seek).expect("Unable to seek to the position");

        let mut buffer = vec![0u8; footer.index_size as usize];
        file.read_exact(&mut buffer)
            .expect("Unable to read the file");

        bincode::deserialize(&buffer).unwrap()
    }
}
