use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::structures::cache::Cache;

#[derive(Debug)]
pub struct MemTable {
    pub tree: BTreeMap<String, Option<String>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            tree: BTreeMap::new(),
        }
    }

    pub fn delete(&mut self, key: &str) {
        self.tree.insert(key.to_owned(), None);
    }

    pub fn add(&mut self, key: &str, value: &str) {
        self.tree.insert(key.to_owned(), Some(value.to_owned()));
    }

    pub fn get(&self, key: &str) -> Option<&Option<String>> {
        self.tree.get(key)
    }

    pub fn len(&self) -> usize {
        self.tree.len()
    }
}
