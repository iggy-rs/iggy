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

# Create a directory for the performance results
(mkdir -p performance_results || true) &> /dev/null

# Construct standard performance suites, each should process 8 GB of data
LARGE_BATCH_ONLY_CACHE_SEND=$(construct_bench_command "$IGGY_BENCH_CMD" "send" 8 1000 1000 1000 tcp "only_cache")  # 8GB data, 1KB messages, 1000 msgs/batch with forced cache
LARGE_BATCH_ONLY_CACHE_POLL=$(construct_bench_command "$IGGY_BENCH_CMD" "poll" 8 1000 1000 1000 tcp "only_cache")  # 8GB data, 1KB messages, 1000 msgs/batch with forced cache

LARGE_BATCH_NO_CACHE_SEND=$(construct_bench_command "$IGGY_BENCH_CMD" "send" 8 1000 1000 1000 tcp "no_cache")  # 8GB data, 1KB messages, 1000 msgs/batch with disabled cache
LARGE_BATCH_NO_CACHE_POLL=$(construct_bench_command "$IGGY_BENCH_CMD" "poll" 8 1000 1000 1000 tcp "no_cache")  # 8GB data, 1KB messages, 1000 msgs/batch with disabled cache

LARGE_BATCH_NO_WAIT_SEND=$(construct_bench_command "$IGGY_BENCH_CMD" "send" 8 1000 1000 1000 tcp "no_wait")  # 8GB data, 1KB messages, 1000 msgs/batch with no_wait config
LARGE_BATCH_NO_WAIT_POLL=$(construct_bench_command "$IGGY_BENCH_CMD" "poll" 8 1000 1000 1000 tcp "no_wait")  # 8GB data, 1KB messages, 1000 msgs/batch with no_wait config

SMALL_BATCH_ONLY_CACHE_SEND=$(construct_bench_command "$IGGY_BENCH_CMD" "send" 8 1000 100 10000 tcp "only_cache")    # 8GB data, 1KB messages, 100 msgs/batch with forced cache
SMALL_BATCH_ONLY_CACHE_POLL=$(construct_bench_command "$IGGY_BENCH_CMD" "poll" 8 1000 100 10000 tcp "only_cache")     # 8GB data, 1KB messages, 100 msgs/batch with forced cache

SMALL_BATCH_NO_CACHE_SEND=$(construct_bench_command "$IGGY_BENCH_CMD" "send" 8 1000 100 10000 tcp "no_cache")    # 8GB data, 1KB messages, 100 msgs/batch, no cache
SMALL_BATCH_NO_CACHE_POLL=$(construct_bench_command "$IGGY_BENCH_CMD" "poll" 8 1000 100 10000 tcp "no_cache")     # 8GB data, 1KB messages, 100 msgs/batch, no cache

LARGE_BATCH_NO_CACHE_SEND_AND_POLL=$(construct_bench_command "$IGGY_BENCH_CMD" "send-and-poll" 8 1000 1000 1000 tcp "no_cache")  # 8GB data, 1KB messages, 1000 msgs/batch with disabled cache
LARGE_BATCH_NO_CACHE_CG_POLL=$(construct_bench_command "$IGGY_BENCH_CMD" "consumer-group-poll" 8 1000 1000 1000 tcp "no_cache")  # 8GB data, 1KB messages, 1000 msgs/batch with disabled cache


# Make an array of the suites
SUITES=(
    "${LARGE_BATCH_ONLY_CACHE_SEND}"
    "${LARGE_BATCH_ONLY_CACHE_POLL}"
    "${LARGE_BATCH_NO_CACHE_SEND}"
    "${LARGE_BATCH_NO_CACHE_POLL}"
    "${LARGE_BATCH_NO_WAIT_SEND}"
    "${LARGE_BATCH_NO_WAIT_POLL}"
    "${SMALL_BATCH_ONLY_CACHE_SEND}"
    "${SMALL_BATCH_ONLY_CACHE_POLL}"
    "${SMALL_BATCH_NO_CACHE_SEND}"
    "${SMALL_BATCH_NO_CACHE_POLL}"
    "${LARGE_BATCH_NO_CACHE_SEND_AND_POLL}"
    "${LARGE_BATCH_NO_CACHE_CG_POLL}"
)

# Run the suites, iterate over two elements at a time
for (( i=0; i<${#SUITES[@]} ; i+=2 )) ; do
    SEND_BENCH="${SUITES[i]}"
    POLL_BENCH="${SUITES[i+1]}"

    # Remove old local_data
    echo "Cleaning old local_data..."
    rm -rf local_data || true

    # Start iggy-server with appropriate configuration
    if [[ "$SEND_BENCH" == *"only_cache"* ]] || [[ "$POLL_BENCH" == *"only_cache"* ]]; then
        echo "Starting iggy-server with command: IGGY_SYSTEM_CACHE_SIZE=\"9GB\" target/release/iggy-server"
        IGGY_SYSTEM_CACHE_SIZE="9GB" target/release/iggy-server &> /dev/null &
    elif [[ "$SEND_BENCH" == *"no_cache"* ]] || [[ "$POLL_BENCH" == *"no_cache"* ]]; then
        echo "Starting iggy-server with command: IGGY_SYSTEM_CACHE_ENABLED=false target/release/iggy-server"
        IGGY_SYSTEM_CACHE_ENABLED=false target/release/iggy-server &> /dev/null &
    elif [[ "$SEND_BENCH" == *"no_wait"* ]] || [[ "$POLL_BENCH" == *"no_wait"* ]]; then
        echo "Starting iggy-server with command: IGGY_SYSTEM_SEGMENT_SERVER_CONFIRMATION=no_wait target/release/iggy-server"
        IGGY_SYSTEM_SEGMENT_SERVER_CONFIRMATION=no_wait target/release/iggy-server &> /dev/null &
    else
        echo "Starting iggy-server with command: target/release/iggy-server"
        target/release/iggy-server &> /dev/null &
    fi
    IGGY_SERVER_PID=$!
    sleep 2

    # Check if the server is running
    exit_if_process_is_not_running "$IGGY_SERVER_PID"

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
