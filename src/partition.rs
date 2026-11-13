use crate::message::Message;
use std::sync::RwLock;

pub struct Partition {
    pub id: u32,
    log: RwLock<Vec<Message>>,
}

impl Partition {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            log: RwLock::new(Vec::new()),
        }
    }

    pub fn append(&self, mut msg: Message) -> u64 {
        let mut log = self.log.write().unwrap();
        let offset = log.len() as u64;
        msg.offset = offset;
        msg.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        log.push(msg);
        offset
    }

    pub fn read_from(&self, from_offset: u64, max_messages: usize) -> Vec<Message> {
        let log = self.log.read().unwrap();
        let start = from_offset as usize;
        if start >= log.len() {
            return Vec::new();
        }
        let end = (start + max_messages).min(log.len());
        log[start..end].to_vec()
    }

    pub fn len(&self) -> u64 {
        self.log.read().unwrap().len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
