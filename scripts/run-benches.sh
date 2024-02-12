#!/bin/bash

# shellcheck disable=SC1091

set -euo pipefail

# Load utility functions
source "$(dirname "$0")/utils.sh"

# Trap SIGINT (Ctrl+C) and execute the on_exit function, do the same on script exit
trap on_exit_bench SIGINT
trap on_exit_bench EXIT

# Remove old local_data
echo "Cleaning old local_data..."
rm -rf local_data

# Build the project
echo "Building project..."
cargo build --release

# Start iggy-server
echo "Running iggy-server..."
target/release/iggy-server &> /dev/null &
sleep 1

# Start tcp send bench
echo "Running iggy-bench send tcp..."
send_results=$(target/release/iggy-bench send tcp | grep -e "Results: total throughput")
sleep 1

# Start tcp poll bench
echo "Running iggy-bench poll tcp..."
poll_results=$(target/release/iggy-bench poll tcp | grep -e "Results: total throughput")

# Gracefully stop the server
send_signal "iggy-server" "TERM"

# Display results
echo
echo "Send results:"
echo "${send_results}"
echo
echo "Poll results:"
echo "${poll_results}"
echo

exit 0
