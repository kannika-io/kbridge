#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v vhs &> /dev/null; then
    echo "Error: vhs not found. Please install: https://github.com/charmbracelet/vhs"
    exit 1
fi

# Build and add to PATH so vhs can find kbridge
cargo build --release
export PATH="$PWD/target/release:$PATH"

# Make sure everything is gone
just teardown

just setup-local-dev

vhs examples/demo.tape
echo "Generated examples/demo.gif"
