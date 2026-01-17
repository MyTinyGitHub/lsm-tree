use crate::structures::{
    key_cache::Cache,
    memtable::MemTable,
    write_ahead_logger::{Operations, WriteAheadLogger},
};
use log::info;
use log4rs::encode::Write;

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

        let _ = self.immutable_memtable.take().unwrap().persist();
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
}
