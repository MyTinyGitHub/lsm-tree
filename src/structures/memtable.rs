use std::collections::BTreeMap;

use crate::structures::bloom_filter::BloomFilter;

#[derive(Debug, Default)]
pub struct MemTable {
    pub tree: BTreeMap<String, Option<String>>,
    pub bloom_filter: BloomFilter,
}

impl MemTable {
    pub fn delete(&mut self, key: &str) {
        self.tree.insert(key.to_owned(), None);
    }

    pub fn add(&mut self, key: &str, value: &str) {
        self.bloom_filter.update(key);
        self.tree.insert(key.to_owned(), Some(value.to_owned()));
    }

    pub fn get(&self, key: &str) -> Option<&Option<String>> {
        self.tree.get(key)
    }

    pub fn len(&self) -> usize {
        self.tree.len()
    }
}
