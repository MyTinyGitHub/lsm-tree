use log::info;

#[derive(Clone, Debug)]
pub struct BloomFilter {
    value: Vec<usize>,
    size: usize,
}

impl BloomFilter {
    pub fn new(size: usize) -> Self {
        Self {
            value: vec![0; size],
            size,
        }
    }

    pub fn persist_value(&self) -> String {
        self.value
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn update(&mut self, value: &str) {
        info!("updating the bloomfilter with key {}", value);

        let val1 = xxhash_rust::xxh3::xxh3_64_with_seed(value.as_bytes(), 1) as usize % self.size;
        let val2 = xxhash_rust::xxh3::xxh3_64_with_seed(value.as_bytes(), 2) as usize % self.size;
        let val3 = xxhash_rust::xxh3::xxh3_64_with_seed(value.as_bytes(), 3) as usize % self.size;

        self.value[val1] = 1;
        self.value[val2] = 1;
        self.value[val3] = 1;
    }

    pub fn contains(&self, value: &str) -> bool {
        info!("searching the bloomfilter for key {}", value);

        let val1 = xxhash_rust::xxh3::xxh3_64_with_seed(value.as_bytes(), 1) as usize % self.size;
        let val2 = xxhash_rust::xxh3::xxh3_64_with_seed(value.as_bytes(), 2) as usize % self.size;
        let val3 = xxhash_rust::xxh3::xxh3_64_with_seed(value.as_bytes(), 3) as usize % self.size;

        let res = self.value[val1] == 1 && self.value[val2] == 1 && self.value[val3] == 1;
        info!("{}", res);
        res
    }
}
