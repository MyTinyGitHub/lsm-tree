use std::{
    fs::{self, OpenOptions},
    io::Write,
};

use serde::{Deserialize, Serialize};

use crate::config::Config;

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub version: usize,
    pub next_id: usize,
    pub mem_tables: Vec<SSTableBasicInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SSTableBasicInfo {
    pub id: usize,
    pub path: String,
    pub level: usize,
    pub min_key: String,
    pub max_key: String,
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
    pub fn create_level_0_filename(&mut self) -> String {
        self.next_id += 1;
        format!(
            "{}/L0_{:010}.sst",
            Config::global().directory.ss_table,
            self.next_id
        )
    }

    pub fn read_from_file() -> Result<Self, Box<dyn std::error::Error>> {
        let file_path = &Config::global().ss_table.manifest_location;
        let file = fs::File::open(file_path)?;
        let result: Self = serde_json::from_reader(file)?;

        Ok(result)
    }

    pub fn persist(&self) {
        let file_path = &Config::global().ss_table.manifest_location;
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(file_path)
            .expect("Unable to create or open the manifest file");

        let serde_bytes = serde_json::to_string_pretty(self).expect("Unable to serde the Manifest");
        file.write_all(serde_bytes.as_bytes())
            .expect("Unable to write to Manifest file");
    }

    pub fn new() -> Self {
        let from_file = Manifest::read_from_file();
        from_file.unwrap_or_else(|_| {
            let result = Manifest::default();

            result.persist();

            result
        })
    }
}
