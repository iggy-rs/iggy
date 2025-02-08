#!/bin/bash

# shellcheck disable=SC1091

IGGY_BENCH_CMD=""
IDENTIFIER=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --identifier)
            IDENTIFIER="$2"
            shift 2
            ;;
        *)
            if [ -z "$IGGY_BENCH_CMD" ]; then
                IGGY_BENCH_CMD="$1"
            fi
            shift
            ;;
    esac
done

# Set default binary path if not provided
if [ -z "$IGGY_BENCH_CMD" ]; then
    IGGY_BENCH_CMD="target/release/iggy-bench"
fi

echo "Using iggy-bench binary: ${IGGY_BENCH_CMD}"
if [ -n "$IDENTIFIER" ]; then
    echo "Using identifier: ${IDENTIFIER}"
else
    echo "Using hostname: $(hostname) as identifier"
fi

set -euo pipefail

# Load utility functions
source "$(dirname "$0")/../utils.sh"
source "$(dirname "$0")/utils.sh"

# Trap SIGINT (Ctrl+C) and execute the on_exit function, do the same on script exit
trap on_exit_bench SIGINT
trap on_exit_bench EXIT

# Function to get environment variables based on benchmark type
get_env_vars() {
    local bench_type="$1"
    local env_vars=()

    # Specific env vars based on bench type
    case "$bench_type" in
        *"only_cache"*)
            env_vars+=("IGGY_SYSTEM_CACHE_SIZE=9GB")
            ;;
        *"no_cache"*)
            env_vars+=("IGGY_SYSTEM_CACHE_ENABLED=false")
            ;;
        *"no_wait"*)
            env_vars+=("IGGY_SYSTEM_SEGMENT_SERVER_CONFIRMATION=no_wait")
            ;;
    esac

    # Convert array to env var string
    local env_string=""
    for var in "${env_vars[@]}"; do
        env_string+="$var "
    done
    echo "$env_string"
}

# Build the project
echo "Building project..."
RUSTFLAGS="-C target-cpu=native" cargo build --release

# Create a directory for the performance results
(mkdir -p performance_results || true) &> /dev/null


##############################
#      Double benchmarks     #
##############################

# Large batch tests with cache enabled
LARGE_BATCH_ONLY_CACHE_PINNED_PRODUCER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-producer" 8 8 1000 1000 1000 tcp "only_cache" "$IDENTIFIER")  # 8GB data, 1KB messages, 1000 msgs/batch with forced cache
LARGE_BATCH_ONLY_CACHE_PINNED_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-consumer" 8 8 1000 1000 1000 tcp "only_cache" "$IDENTIFIER")  # 8GB data, 1KB messages, 1000 msgs/batch with forced cache

# Large batch tests with cache disabled
LARGE_BATCH_NO_CACHE_PINNED_PRODUCER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-producer" 8 8 1000 1000 1000 tcp "no_cache" "$IDENTIFIER")  # 8GB data, 1KB messages, 1000 msgs/batch with disabled cache
LARGE_BATCH_NO_CACHE_PINNED_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-consumer" 8 8 1000 1000 1000 tcp "no_cache" "$IDENTIFIER")  # 8GB data, 1KB messages, 1000 msgs/batch with disabled cache

# Large batch tests with no wait configuration
LARGE_BATCH_NO_WAIT_PINNED_PRODUCER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-producer" 8 8 1000 1000 1000 tcp "no_wait" "$IDENTIFIER")  # 8GB data, 1KB messages, 1000 msgs/batch with no_wait config
LARGE_BATCH_NO_WAIT_PINNED_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-consumer" 8 8 1000 1000 1000 tcp "no_wait" "$IDENTIFIER")  # 8GB data, 1KB messages, 1000 msgs/batch with no_wait config

# Small batch tests with cache enabled
SMALL_BATCH_ONLY_CACHE_PINNED_PRODUCER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-producer" 8 8 1000 100 10000 tcp "only_cache" "$IDENTIFIER")    # 8GB data, 1KB messages, 100 msgs/batch with forced cache
SMALL_BATCH_ONLY_CACHE_PINNED_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-consumer" 8 8 1000 100 10000 tcp "only_cache" "$IDENTIFIER")     # 8GB data, 1KB messages, 100 msgs/batch with forced cache

# Consumer group tests with cache enabled
BALANCED_ONLY_CACHE_PRODUCER=$(construct_bench_command "$IGGY_BENCH_CMD" "balanced-producer" 1 8 100 1000 10000 tcp "only_cache" "$IDENTIFIER")  # Balanced producer benchmark
BALANCED_ONLY_CACHE_CONSUMER_GROUP=$(construct_bench_command "$IGGY_BENCH_CMD" "balanced-consumer-group" 1 8 100 1000 10000 tcp "only_cache" "$IDENTIFIER")  # Consumer group benchmark

# Consumer group tests with cache disabled
BALANCED_NO_CACHE_PRODUCER=$(construct_bench_command "$IGGY_BENCH_CMD" "balanced-producer" 1 8 100 1000 10000 tcp "no_cache" "$IDENTIFIER")  # Balanced producer benchmark
BALANCED_NO_CACHE_CONSUMER_GROUP=$(construct_bench_command "$IGGY_BENCH_CMD" "balanced-consumer-group" 1 8 100 1000 10000 tcp "no_cache" "$IDENTIFIER")  # Consumer group benchmark

###############################
#      Single benchmarks      #
###############################

# Parallel producer and consumer
LARGE_BATCH_NO_CACHE_PINNED_PRODUCER_AND_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-producer-and-consumer" 8 8 1000 1000 1000 tcp "no_cache" "$IDENTIFIER")    # 8GB data, 1KB messages, 100 msgs/batch, no cache
LARGE_BATCH_ONLY_CACHE_PINNED_PRODUCER_AND_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-producer-and-consumer" 8 8 1000 1000 1000 tcp "only_cache" "$IDENTIFIER")     # 8GB data, 1KB messages, 100 msgs/batch, no cache

# Parallel consumer group test
BALANCED_NO_CACHE_CONSUMER_GROUP_PRODUCER_AND_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "balanced-producer-and-consumer-group" 1 8 100 1000 10000 tcp "no_cache" "$IDENTIFIER")
BALANCED_ONLY_CACHE_CONSUMER_GROUP_PRODUCER_AND_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "balanced-producer-and-consumer-group" 1 8 100 1000 10000 tcp "only_cache" "$IDENTIFIER")

# End-to-end tests
END_TO_END_NO_CACHE_PRODUCING_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "end-to-end-producing-consumer" 1 8 100 1000 10000 tcp "no_cache" "$IDENTIFIER")  # Combined producer and consumer benchmark
END_TO_END_ONLY_CACHE_PRODUCING_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "end-to-end-producing-consumer" 1 8 100 1000 10000 tcp "only_cache" "$IDENTIFIER")  # Combined producer and consumer benchmark

# Make an array of the suites
DOUBLE_SUITES=(
    "$LARGE_BATCH_ONLY_CACHE_PINNED_PRODUCER"
    "$LARGE_BATCH_ONLY_CACHE_PINNED_CONSUMER"
    "$LARGE_BATCH_NO_CACHE_PINNED_PRODUCER"
    "$LARGE_BATCH_NO_CACHE_PINNED_CONSUMER"
    "$LARGE_BATCH_NO_WAIT_PINNED_PRODUCER"
    "$LARGE_BATCH_NO_WAIT_PINNED_CONSUMER"
    "$SMALL_BATCH_ONLY_CACHE_PINNED_PRODUCER"
    "$SMALL_BATCH_ONLY_CACHE_PINNED_CONSUMER"
    "$BALANCED_ONLY_CACHE_PRODUCER"
    "$BALANCED_ONLY_CACHE_CONSUMER_GROUP"
    "$BALANCED_NO_CACHE_PRODUCER"
    "$BALANCED_NO_CACHE_CONSUMER_GROUP"
)

SINGLE_SUITES=(
    "$LARGE_BATCH_NO_CACHE_PINNED_PRODUCER_AND_CONSUMER"
    "$LARGE_BATCH_ONLY_CACHE_PINNED_PRODUCER_AND_CONSUMER"
    "$END_TO_END_ONLY_CACHE_PRODUCING_CONSUMER"
    "$END_TO_END_NO_CACHE_PRODUCING_CONSUMER"
    "$BALANCED_ONLY_CACHE_CONSUMER_GROUP_PRODUCER_AND_CONSUMER"
    "$BALANCED_NO_CACHE_CONSUMER_GROUP_PRODUCER_AND_CONSUMER"
)

echo
echo "Running single benchmark tests..."
echo

# Run each single benchmark suites
for suite in "${SINGLE_SUITES[@]}"; do
    # Remove old local_data
    echo "Cleaning old local_data..."
    rm -rf local_data || true

    # Get environment variables based on benchmark type
    ENV_VARS=$(get_env_vars "$suite")

    echo "Starting iggy-server with command:"

    # Start iggy-server with appropriate configuration
    if [[ -n "$ENV_VARS" ]]; then
        echo "$ENV_VARS target/release/iggy-server"
        eval "$ENV_VARS target/release/iggy-server" &> /dev/null &
    else
        echo "target/release/iggy-server"
        target/release/iggy-server &> /dev/null &
    fi
    echo
    IGGY_SERVER_PID=$!
    sleep 2

    # Check if the server is running
    exit_if_process_is_not_running "$IGGY_SERVER_PID"

    # Run the benchmark
    echo "Running benchmark:"
    echo "$ENV_VARS ${suite}"
    results=$(eval "$ENV_VARS ${suite}" | grep -e "Results:")
    echo
    echo "Results:"
    echo "${results}"
    echo
    sleep 1

    # Gracefully stop the server
    echo "Stopping iggy-server..."
    send_signal "iggy-server" "TERM"
done

echo
echo "Running double benchmark tests..."
echo

# Run the double benchmark suites, iterate over two elements at a time
for (( i=0; i<${#DOUBLE_SUITES[@]} ; i+=2 )) ; do
    PRODUCER_BENCH="${DOUBLE_SUITES[i]}"
    CONSUMER_BENCH="${DOUBLE_SUITES[i+1]}"

    # Remove old local_data
    echo "Cleaning old local_data..."
    rm -rf local_data || true

    # Get environment variables based on benchmark type
    ENV_VARS=$(get_env_vars "$PRODUCER_BENCH")

    echo "Starting iggy-server with command:"

    # Start iggy-server with appropriate configuration
    if [[ -n "$ENV_VARS" ]]; then
        echo "$ENV_VARS target/release/iggy-server"
        eval "$ENV_VARS target/release/iggy-server" &> /dev/null &
    else
        echo "target/release/iggy-server"
        target/release/iggy-server &> /dev/null &
    fi
    echo
    IGGY_SERVER_PID=$!
    sleep 2

    # Check if the server is running
    exit_if_process_is_not_running "$IGGY_SERVER_PID"

    # Start producer bench
    echo "Running producer bench:"
    echo "$ENV_VARS ${PRODUCER_BENCH}"
    producer_results=$(eval "$ENV_VARS ${PRODUCER_BENCH}" | grep -e "Results:")
    echo
    echo "Producer results:"
    echo "${producer_results}"
    echo
    sleep 1

    # Start consumer bench
    echo "Running consumer bench:"
    echo "$ENV_VARS ${CONSUMER_BENCH}"
    consumer_results=$(eval "$ENV_VARS ${CONSUMER_BENCH}" | grep -e "Results:")
    echo
    echo "Consumer results:"
    echo "${consumer_results}"
    echo
    sleep 1

    # Gracefully stop the server
    echo "Stopping iggy-server..."
    send_signal "iggy-server" "TERM"
done

exit 0
