# Strata-Queue

[![License: Noncommercial](https://img.shields.io/badge/License-Noncommercial-blue.svg)](LICENSE)

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
[Noncommercial license](LICENSE) © [dimxxkun](https://github.com/dimxxkun)

## Commercial permission

Original material in this repository is available under the [Noncommercial Use and Commercial Permission License](LICENSE). **Noncommercial use is free. Commercial use requires prior written permission and a separate written agreement specifying the scope and agreed fee.** Contact [work.abdulfatai@gmail.com](mailto:work.abdulfatai@gmail.com) with the repository name and intended use.

Portfolio review and recruitment evaluation are permitted without a fee. Third-party components retain their own licenses. Permissions already granted for earlier versions remain valid; the new terms do not revoke those grants. These are custom source-available terms, not an OSI-approved open-source license.
