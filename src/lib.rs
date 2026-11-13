pub mod broker;
pub mod consumer_group;
pub mod message;
pub mod partition;

pub use broker::{StrataBroker, Topic};
pub use consumer_group::ConsumerGroup;
pub use message::Message;
pub use partition::Partition;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_publish_and_consume() {
        let broker = StrataBroker::new(3);
        broker.create_topic("orders", 2);

        let (part, offset) = broker.publish("orders", "cust-1", b"order_data_1").unwrap();
        assert_eq!(offset, 0);

        let messages = broker.poll("orders", "billing-group", part, 10);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].key, "cust-1");

        // Commit offset
        broker.ack("billing-group", part, messages[0].offset);

        // Next poll should return empty
        let next_messages = broker.poll("orders", "billing-group", part, 10);
        assert!(next_messages.is_empty());
    }

    #[test]
    fn test_dead_letter_queue() {
        let broker = StrataBroker::new(3);
        let mut msg = Message::new("poison-key", b"malformed_payload");

        // Fail 3 times -> goes to DLQ
        broker.nack(msg.clone());
        msg.delivery_attempts += 1;
        broker.nack(msg.clone());
        msg.delivery_attempts += 1;
        broker.nack(msg.clone());

        assert_eq!(broker.dlq_len(), 1);
    }
}
