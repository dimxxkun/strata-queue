use std::collections::HashMap;
use std::sync::RwLock;

pub struct ConsumerGroup {
    pub group_id: String,
    // Maps partition_id -> committed offset
    offsets: RwLock<HashMap<u32, u64>>,
}

impl ConsumerGroup {
    pub fn new(group_id: &str) -> Self {
        Self {
            group_id: group_id.to_string(),
            offsets: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_offset(&self, partition_id: u32) -> u64 {
        let offsets = self.offsets.read().unwrap();
        *offsets.get(&partition_id).unwrap_or(&0)
    }

    pub fn commit_offset(&self, partition_id: u32, offset: u64) {
        let mut offsets = self.offsets.write().unwrap();
        offsets.insert(partition_id, offset);
    }
}
