# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kannika Bridge is a Kafka consumer group offset migration tool written in Rust. It migrates consumer group offsets between clusters through a 3-step workflow:
1. **Fetch** - Retrieve committed offsets from source cluster
2. **Calculate/Transform** - Map source offsets to target offsets using message headers
3. **Apply** - Commit transformed offsets to target cluster

## Build Commands

```bash
cargo build                      # Debug build
cargo build --release            # Release build
cargo fmt --all                  # Format code
cargo fmt --all -- --check       # Check formatting (CI)
cargo test --verbose             # Run all tests
cargo test <test_name>           # Run single test by name
cargo doc --no-deps              # Generate documentation
```

## Task Runner (just)

```bash
just setup-ci          # Set up Docker Kafka clusters with test data
just setup-local-dev   # Setup CI + local dev containers
just teardown          # Tear down Docker compose environment
just run-example       # Full e2e example (setup + apply)
just run-docs          # Generate and serve documentation
```

## Running the CLI

```bash
cargo run -- fetch -b localhost:9092                    # Fetch offsets
cargo run -- calculate -b localhost:9093 -H Offset -i - # Calculate from stdin
cargo run -- apply -b localhost:9093 --from-stdin       # Apply from stdin
```

## Architecture

### Workspace Structure

- **bridge-cli/** - CLI binary using clap for argument parsing
- **bridge-core/** - Core library with reusable logic

### Key Modules (bridge-core/src/)

- `client/` - `BridgeClient` trait and `KafkaBridgeClient` implementation
- `commands/` - `fetch_source_offsets` and `apply_target_offsets` command implementations
- `kafka/` - Kafka wrapper layer around rdkafka (admin, consumer, config)
- `transform/` - `ConsumerGroupMigrator` for offset transformation with binary search optimization
- `snapshot/` - `OffsetSnapshot` and CSV serialization
- `partition/` - Partition abstraction with mock implementation for testing

### Core Abstractions

The `BridgeClient` trait defines the main operations:
```rust
async fn fetch_source_offsets_from_cluster(...) -> Result<OffsetSnapshot>;
async fn calculate_target_offsets(...) -> Result<OffsetSnapshot>;
async fn apply_target_offsets(...) -> Result<()>;
```

Type aliases in `prelude.rs`:
```rust
type Topic = String;
type Offset = i64;
type ConsumerGroup = String;
```

## Testing

Integration tests are in `bridge-core/tests/scenarios/`:
- `fetch_offsets.rs` - Fetch operations
- `calculate.rs` - Offset transformation
- `apply_target_offsets.rs` - Applying offsets

Tests use `testcontainers` for containerized Kafka clusters. Some tests require `just setup-ci` to be run first. Tests marked with `#[serial_test]` cannot run in parallel.

## Code Conventions

- Rust Edition 2024, max line width 100 chars (rustfmt.toml)
- All I/O operations use tokio async runtime
- Errors use custom enums with `thiserror`
- Use `#[async_trait]` for async trait methods
- CSV format: `consumer_group,topic,partition,offset`
- Stdin input via `-i -` or `--from-stdin` flags
