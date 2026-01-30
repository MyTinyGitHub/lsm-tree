use std::{fs, sync::OnceLock};

use log::info;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub wal: WALConfig,
    pub memtable: MemTableConfig,
    pub directory: Directories,
    pub cache: CacheConfig,
    pub ss_table: SSTableConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MemTableConfig {
    pub max_entries: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SSTableConfig {
    pub manifest_location: String,
    pub l0_file_count_limit: usize,
    pub l1_file_size_upper_limit: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WALConfig {
    pub version: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CacheConfig {
    pub index_size: usize,
    pub bloom_filter_size: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Directories {
    pub log: String,
    pub wal: String,
    pub ss_table: String,
}

pub static CONFIG: OnceLock<Config> = OnceLock::new();

impl Config {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let env = std::env::var("ENV").unwrap_or_else(|_| "dev".to_owned());

        info!("Loading {} configuration environment", env);

        let file_name = format!("config.{}.toml", env);

        let content = fs::read_to_string(file_name)?;
        let config: Config = toml::from_str(&content)?;

        let _ = fs::create_dir_all(&config.directory.wal);
        let _ = fs::create_dir_all(&config.directory.ss_table);

        Ok(config)
    }

    pub fn load_test() -> Result<Self, Box<dyn std::error::Error>> {
        info!("Loading test configuration environment");
        let file_name = "config.test.toml".to_owned();

        let content = fs::read_to_string(file_name)?;
        let config: Config = toml::from_str(&content)?;

        let _ = fs::create_dir_all(&config.directory.wal);
        let _ = fs::create_dir_all(&config.directory.ss_table);

        Ok(config)
    }

    pub fn global() -> &'static Config {
        CONFIG.get_or_init(|| Self::load().expect("Failed to load config"))
    }

    pub fn test() -> &'static Config {
        info!("Loading test configurations");
        CONFIG.get_or_init(|| Self::load_test().expect("Failed to load config"))
    }
}
