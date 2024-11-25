#!/bin/bash

# shellcheck disable=SC1091

IGGY_BENCH_CMD=""
if [ -z "$1" ]; then
    IGGY_BENCH_CMD="target/release/iggy-bench"
else
    IGGY_BENCH_CMD="$1"
fi

echo "Using iggy-bench binary: ${IGGY_BENCH_CMD}"

set -euo pipefail

# Load utility functions
source "$(dirname "$0")/../utils.sh"
source "$(dirname "$0")/utils.sh"


# Trap SIGINT (Ctrl+C) and execute the on_exit function, do the same on script exit
trap on_exit_bench SIGINT
trap on_exit_bench EXIT

# Build the project
echo "Building project..."
cargo build --release

# Remove old performance results
echo "Cleaning old performance results..."
rm -rf performance_results || true

# Construct standard performance suites, each should process 8 GB of data
STANDARD_SEND=$(construct_bench_command "$IGGY_BENCH_CMD" "send" 8 1000 1000 1000 tcp)        # 8 producers, 8 streams, 1000 byte messages, 1000 messages per batch, 1000 message batches, tcp
STANDARD_POLL=$(construct_bench_command "$IGGY_BENCH_CMD" "poll" 8 1000 1000 1000 tcp)         # 8 consumers, 8 streams, 1000 byte messages, 1000 messages per batch, 1000 message batches, tcp
SMALL_BATCH_SEND=$(construct_bench_command "$IGGY_BENCH_CMD" "send" 8 1000 100 10000 tcp)    # 8 producers, 8 streams, 1000 byte messages, 100 messages per batch, 10000 message batches, tcp
SMALL_BATCH_POLL=$(construct_bench_command "$IGGY_BENCH_CMD" "poll" 8 1000 100 10000 tcp)     # 8 consumers, 8 streams, 1000 byte messages, 100 messages per batch, 10000 message batches, tcp

# SMALL_BATCH_SMALL_MSG_SEND=$(construct_bench_command "$IGGY_BENCH_CMD" "send" 8 20 100 500000 tcp)    # Uncomment and adjust if needed
# SMALL_BATCH_SMALL_MSG_POLL=$(construct_bench_command "$IGGY_BENCH_CMD" "poll" 8 20 100 500000 tcp)  # Uncomment and adjust if needed
# SINGLE_MESSAGE_BATCH_SMALL_MSG_SEND=$(construct_bench_command "$IGGY_BENCH_CMD" "send" 8 20 1 50000000 tcp)  # Uncomment and adjust if needed
# SINGLE_MESSAGE_BATCH_SMALL_MSG_POLL=$(construct_bench_command "$IGGY_BENCH_CMD" "poll" 8 20 1 50000000 tcp)  # Uncomment and adjust if needed

# Make an array of the suites
SUITES=(
    "${STANDARD_SEND}"
    "${STANDARD_POLL}"
    "${SMALL_BATCH_SEND}"
    "${SMALL_BATCH_POLL}"
    # "${SMALL_BATCH_SMALL_MSG_SEND}"
    # "${SMALL_BATCH_SMALL_MSG_POLL}"
)

# Run the suites, iterate over two elements at a time
for (( i=0; i<${#SUITES[@]} ; i+=2 )) ; do
    SEND_BENCH="${SUITES[i]}"
    POLL_BENCH="${SUITES[i+1]}"

    # Remove old local_data
    echo "Cleaning old local_data..."
    rm -rf local_data || true

    # Start iggy-server
    echo "Starting iggy-server..."
    target/release/iggy-server &> /dev/null &
    sleep 2
    echo

    # Start send bench
    echo "Running ${SEND_BENCH}"
    send_results=$(eval "${SEND_BENCH}" | grep -e "Results:")
    echo
    echo "Send results:"
    echo "${send_results}"
    echo
    sleep 1

    # Start poll bench
    echo "Running ${POLL_BENCH}"
    poll_results=$(eval "${POLL_BENCH}" | grep -e "Results:")
    echo
    echo "Poll results:"
    echo "${poll_results}"
    echo
    sleep 1

    # Gracefully stop the server
    echo "Stopping iggy-server..."
    send_signal "iggy-server" "TERM"
done

exit 0
