use std::fs;

use serde::{Deserialize, Serialize};

use crate::config::Config;

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    version: usize,
    next_id: usize,
    mem_tables: Vec<SSTableBasicInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SSTableBasicInfo {
    id: usize,
    path: String,
    level: usize,
    min_key: String,
    max_key: String,
}

impl SSTableBasicInfo {}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            version: 1,
            next_id: 1,
            mem_tables: Vec::new(),
        }
    }
}

impl Manifest {
    pub fn read_from_file() -> Result<Self, Box<dyn std::error::Error>> {
        let file_path = &Config::global().ss_table.manifest_location;
        let file = fs::File::open(file_path)?;
        let result: Self = serde_json::from_reader(file)?;

        Ok(result)
    }

    pub fn new() -> Self {
        let from_file = Manifest::read_from_file();
        from_file.unwrap_or_else(|_| Manifest::default())
    }
}
