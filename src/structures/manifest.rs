use std::{
    fs::{self, OpenOptions},
    io::Write,
};

use log::trace;
use serde::{Deserialize, Serialize};

use crate::config::Config;

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    version: usize,
    next_id: usize,
    ss_tables: Vec<SSTableBasicInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableBasicInfo {
    pub id: usize,
    pub path: String,
    pub level: usize,
    pub min_key: String,
    pub max_key: String,
}

impl SSTableBasicInfo {
    pub fn new(id: usize, path: String, level: usize, min_key: String, max_key: String) -> Self {
        Self {
            id,
            path,
            level,
            min_key,
            max_key,
        }
    }
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            version: 1,
            next_id: 1,
            ss_tables: Vec::new(),
        }
    }
}

impl Manifest {
    pub fn create_filename(&mut self, level: usize) -> (String, usize) {
        let id = self.next_id;
        self.next_id += 1;

        let path = format!(
            "{}/L{}_{:010}.sst",
            Config::global().directory.ss_table,
            level,
            id
        );

        (path, id)
    }

    pub fn remove(&mut self, table: &SSTableBasicInfo) {
        self.ss_tables.retain(|t| t.id != table.id);
    }

    pub fn compaction_nominees(&mut self, level: usize) -> Vec<SSTableBasicInfo> {
        let mut result = self.ss_tables_in_level(0);

        result.sort_by_key(|t| t.id);

        trace!("{} ss_tables found for level: {}", result.len(), level);

        result
            .into_iter()
            .take(2)
            .cloned()
            .collect::<Vec<SSTableBasicInfo>>()
    }

    pub fn add(&mut self, ss_table: SSTableBasicInfo) {
        self.ss_tables.push(ss_table);
        self.persist();
    }

    pub fn ss_tables_in_level(&self, level: usize) -> Vec<&SSTableBasicInfo> {
        trace!("searching ss_tables for level: {}", level);

        let result = self
            .ss_tables
            .iter()
            .filter(|t| t.level == level)
            .collect::<Vec<&SSTableBasicInfo>>();

        trace!("{} ss_tables found for level: {}", result.len(), level);

        result
    }

    pub fn read_from_file() -> Result<Self, Box<dyn std::error::Error>> {
        let file_path = &Config::global().ss_table.manifest_location;
        let file = fs::File::open(file_path)?;
        let result: Self = serde_json::from_reader(file)?;

        Ok(result)
    }

    pub fn persist(&mut self) {
        let file_path = &Config::global().ss_table.manifest_location;
        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .truncate(false)
            .open(file_path)
            .expect("Unable to create or open the manifest file");

        let serde_bytes = serde_json::to_string_pretty(self).expect("Unable to serde the Manifest");
        file.write_all(serde_bytes.as_bytes())
            .expect("Unable to write to Manifest file");
    }

    pub fn new() -> Self {
        let from_file = Manifest::read_from_file();
        from_file.unwrap_or_else(|_| {
            let mut result = Manifest::default();

            result.persist();

            result
        })
    }
}
