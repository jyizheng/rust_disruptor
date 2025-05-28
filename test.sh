#!/bin/bash

cargo run --release  --bin basic_main
cargo run --release  --bin perf_affinity_main
cargo run --release  --bin perf_main
cargo run --release  --bin spsc_basic_main
cargo run --release  --bin three_to_one_main
cargo run --release  --bin wait_strategy_perf_main --features phased_backoff_test
