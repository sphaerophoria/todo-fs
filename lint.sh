#!/usr/bin/env bash

set -ex
cargo +nightly fmt -- --check
cargo clippy -- -Dwarnings -D"clippy::unwrap_used"
cargo test
