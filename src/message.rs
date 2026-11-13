#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub offset: u64,
    pub key: String,
    pub payload: Vec<u8>,
    pub timestamp: u64,
    pub delivery_attempts: u32,
}

impl Message {
    pub fn new(key: &str, payload: &[u8]) -> Self {
        Self {
            offset: 0,
            key: key.to_string(),
            payload: payload.to_vec(),
            timestamp: 0,
            delivery_attempts: 0,
        }
    }
}
