use crate::structures::memtable::MemTable;
use std::{fs::OpenOptions, io::Write};

impl MemTable {
    pub fn persist(&mut self) -> Result<(), ()> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(format!("data/memtables/{}.txt", "1"))
            .ok()
            .ok_or(())?;

        self.tree.iter().for_each(|e| {
            let value = match e.1 {
                Some(v) => v,
                None => "THOMBSTONE NONE",
            };

            let _ = file.write_all(format!("key {} values {}", e.0, value).as_bytes());
        });

        Ok(())
    }
}
