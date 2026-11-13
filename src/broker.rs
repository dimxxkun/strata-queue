use crate::consumer_group::ConsumerGroup;
use crate::message::Message;
use crate::partition::Partition;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct Topic {
    pub name: String,
    partitions: Vec<Arc<Partition>>,
}

impl Topic {
    pub fn new(name: &str, partition_count: u32) -> Self {
        let count = partition_count.max(1);
        let mut partitions = Vec::with_capacity(count as usize);
        for i in 0..count {
            partitions.push(Arc::new(Partition::new(i)));
        }
        Self {
            name: name.to_string(),
            partitions,
        }
    }

    pub fn partition(&self, key: &str) -> Arc<Partition> {
        let mut hash: u32 = 0;
        for b in key.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(b as u32);
        }
        let idx = (hash as usize) % self.partitions.len();
        Arc::clone(&self.partitions[idx])
    }

    pub fn partition_by_id(&self, id: u32) -> Option<Arc<Partition>> {
        self.partitions.get(id as usize).cloned()
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }
}

pub struct StrataBroker {
    topics: RwLock<HashMap<String, Arc<Topic>>>,
    consumer_groups: RwLock<HashMap<String, Arc<ConsumerGroup>>>,
    dlq: Arc<Partition>,
    max_delivery_attempts: u32,
}

impl StrataBroker {
    pub fn new(max_delivery_attempts: u32) -> Self {
        Self {
            topics: RwLock::new(HashMap::new()),
            consumer_groups: RwLock::new(HashMap::new()),
            dlq: Arc::new(Partition::new(9999)),
            max_delivery_attempts,
        }
    }

    pub fn create_topic(&self, name: &str, partitions: u32) -> Arc<Topic> {
        let mut topics = self.topics.write().unwrap();
        let topic = Arc::new(Topic::new(name, partitions));
        topics.insert(name.to_string(), Arc::clone(&topic));
        topic
    }

    pub fn get_topic(&self, name: &str) -> Option<Arc<Topic>> {
        self.topics.read().unwrap().get(name).cloned()
    }

    pub fn publish(&self, topic_name: &str, key: &str, payload: &[u8]) -> Option<(u32, u64)> {
        let topic = self.get_topic(topic_name)?;
        let partition = topic.partition(key);
        let msg = Message::new(key, payload);
        let part_id = partition.id;
        let offset = partition.append(msg);
        Some((part_id, offset))
    }

    pub fn get_or_create_group(&self, group_id: &str) -> Arc<ConsumerGroup> {
        let mut groups = self.consumer_groups.write().unwrap();
        groups
            .entry(group_id.to_string())
            .or_insert_with(|| Arc::new(ConsumerGroup::new(group_id)))
            .clone()
    }

    pub fn poll(
        &self,
        topic_name: &str,
        group_id: &str,
        partition_id: u32,
        batch_size: usize,
    ) -> Vec<Message> {
        let topic = match self.get_topic(topic_name) {
            Some(t) => t,
            None => return Vec::new(),
        };

        let partition = match topic.partition_by_id(partition_id) {
            Some(p) => p,
            None => return Vec::new(),
        };

        let group = self.get_or_create_group(group_id);
        let current_offset = group.get_offset(partition_id);
        partition.read_from(current_offset, batch_size)
    }

    pub fn ack(&self, group_id: &str, partition_id: u32, offset: u64) {
        let group = self.get_or_create_group(group_id);
        group.commit_offset(partition_id, offset + 1);
    }

    pub fn nack(&self, mut msg: Message) {
        msg.delivery_attempts += 1;
        if msg.delivery_attempts >= self.max_delivery_attempts {
            // Poison message -> Divert to Dead-Letter Queue
            self.dlq.append(msg);
        }
    }

    pub fn dlq_len(&self) -> u64 {
        self.dlq.len()
    }
}
