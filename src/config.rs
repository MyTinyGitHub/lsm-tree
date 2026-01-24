use std::{fs, sync::OnceLock};

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub wal: WALConfig,
    pub memtable: MemTableConfig,
    pub directory: Directories,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MemTableConfig {
    pub max_entries: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WALConfig {
    pub version: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Directories {
    pub log: String,
    pub wal: String,
    pub ss_table: String,
}

pub static CONFIG: OnceLock<Config> = OnceLock::new();

impl Default for Config {
    fn default() -> Self {
        Self {
            wal: WALConfig { version: 1 },
            memtable: MemTableConfig { max_entries: 5 },
            directory: Directories {
                log: "log/config/log4rs.yaml".to_owned(),
                wal: "data/wals/".to_owned(),
                ss_table: "data/ss_tables/".to_owned(),
            },
        }
    }
}

impl Config {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let env = std::env::var("ENV").unwrap_or_else(|_| "dev".to_owned());
        let file_name = format!("config.{}.toml", env);

        let content = fs::read_to_string(file_name)?;
        let config: Config = toml::from_str(&content)?;

        Ok(config)
    }

    pub fn global() -> &'static Config {
        CONFIG.get_or_init(|| Self::load().expect("Failed to load config"))
    }

    pub fn inject(config: Config) -> &'static Config {
        CONFIG.get_or_init(|| config)
    }
}
