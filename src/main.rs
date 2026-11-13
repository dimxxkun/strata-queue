use strata_queue::StrataBroker;
use std::sync::Arc;
use std::thread;

fn main() {
    println!("=======================================================");
    println!("   Strata-Queue: Partitioned Pub/Sub Broker Engine     ");
    println!("=======================================================");

    let broker = Arc::new(StrataBroker::new(3));
    let topic_name = "payment-events";
    let partitions = 4;

    broker.create_topic(topic_name, partitions);
    println!("[1] Topic '{}' created with {} partitions.", topic_name, partitions);

    // 1. Concurrently publish 100 messages
    println!("\n[2] Publishing 100 events across partitions concurrently...");
    let mut handles = vec![];

    for producer_id in 0..4 {
        let b = Arc::clone(&broker);
        handles.push(thread::spawn(move || {
            for i in 0..25 {
                let key = format!("merchant-{}-cust-{}", producer_id, i);
                let payload = format!("{{\"event\":\"PAYMENT_SUCCESS\",\"amount\":{}}}", 10 + i);
                b.publish(topic_name, &key, payload.as_bytes());
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
    println!("    Finished producing 100 events.");

    // 2. Consume from Consumer Group across partitions
    println!("\n[3] Consumer Group 'analytics-cluster' polling partitions:");
    let group = "analytics-cluster";
    let mut total_consumed = 0;

    for p in 0..partitions {
        let messages = broker.poll(topic_name, group, p, 50);
        println!("    • Partition {}: read {} messages", p, messages.len());
        for m in &messages {
            broker.ack(group, p, m.offset);
        }
        total_consumed += messages.len();
    }
    println!("    Total acknowledged: {} messages", total_consumed);

    // 3. Poison Message DLQ Simulation
    println!("\n[4] Simulating poison pill message (exceeding 3 max delivery attempts)...");
    let mut poison_msg = strata_queue::Message::new("bad-order", b"corrupted JSON binary");
    for attempt in 1..=3 {
        println!("    Delivery attempt #{}: processing failed, emitting NACK...", attempt);
        broker.nack(poison_msg.clone());
        poison_msg.delivery_attempts += 1;
    }
    println!("    Current Dead-Letter Queue (DLQ) depth: {} (Diverted safely!)", broker.dlq_len());

    println!("\n=======================================================");
    println!("Strata-Queue message engine test concluded successfully.");
}
