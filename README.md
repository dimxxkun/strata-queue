# Strata-Queue

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Strata-Queue** is a high-throughput partitioned event broker implemented in modern Rust. It features partitioned topics with hash routing, monotonic sequence offset tracking, consumer groups with commit state isolation, and a Dead-Letter Queue (DLQ) for poisoned message containment.

---

## Architectural Topology

```
                  Publish(key, payload)
                           │
                           ▼
               [ Hash Partition Router ]
                           │
         ┌─────────────────┼─────────────────┐
         ▼                 ▼                 ▼
   Partition 0       Partition 1       Partition 2
   (Append Log)      (Append Log)      (Append Log)
   [0, 1, 2...]      [0, 1, 2...]      [0, 1, 2...]
         │                 │                 │
         └─────────────────┼─────────────────┘
                           │
                           ▼
          [ Consumer Group Offset Tracking ]
          (Committed offsets per partition)
                           │
                           ▼ (3x Consecutive Failures)
               [ Dead-Letter Queue (DLQ) ]
```

## Engineering Highlights

- **Partitioned Hashing**: Deterministic key hashing ensures order preservation for identical keys while enabling horizontal consumer scaling across partitions.
- **Consumer Group State Machine**: Manages consumer progression independently across consumer groups via safe RwLock guards.
- **Dead-Letter Queue Safeguards**: Poisoned messages that fail processing beyond configurable retry thresholds are quarantined without blocking head-of-line traffic.

## Quick Start & Verification

```bash
# Run unit tests and verifiers
cargo test

# Run multi-threaded producer/consumer benchmark
cargo run
```

## License
MIT © [dimxxkun](https://github.com/dimxxkun)
