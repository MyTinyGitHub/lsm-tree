pub struct Config {
    pub wal: WALConfig,
    pub memtable: MemTableConfig,
    pub directory: Directories,
}

pub struct MemTableConfig {
    pub max_entries: usize,
}

pub struct WALConfig {
    pub version: usize,
}

pub struct Directories {
    pub log: String,
    pub wal: String,
    pub ss_table: String,
    pub cache: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            wal: WALConfig { version: 1 },
            memtable: MemTableConfig { max_entries: 5 },
            directory: Directories {
                log: "log/config/log4rs.yaml".to_owned(),
                wal: "data/wals/".to_owned(),
                ss_table: "data/ss_tables/".to_owned(),
                cache: "data/cache".to_owned(),
            },
        }
    }
}

impl Config {
    pub fn test() -> Self {
        Self {
            wal: WALConfig { version: 1 },
            memtable: MemTableConfig { max_entries: 5 },
            directory: Directories {
                log: "log/config/log4rs.yaml".to_owned(),
                wal: "test-data/wals/".to_owned(),
                ss_table: "test-data/ss_tables/".to_owned(),
                cache: "test-data/cache".to_owned(),
            },
        }
    }
}
