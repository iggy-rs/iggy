#!/bin/bash

# shellcheck disable=SC1091

set -euo pipefail

# Load utility functions
source "$(dirname "$0")/utils.sh"

# Trap SIGINT (Ctrl+C) and execute the on_exit function
trap on_exit_profile SIGINT
trap on_exit_profile EXIT

PROFILED_APP_NAME=$1

if [[ -z "${PROFILED_APP_NAME}" ]]; then
    echo "Usage: $0 <app_name>"
    exit 1
fi

if [[ "${PROFILED_APP_NAME}" != "iggy-bench" ]] && [[ "${PROFILED_APP_NAME}" != "iggy-server" ]]; then
    echo "Invalid app name. Please use 'iggy-bench' or 'iggy-server'."
    exit 1
fi

# Check system settings for perf
paranoid=$(cat /proc/sys/kernel/perf_event_paranoid)
restrict=$(cat /proc/sys/kernel/kptr_restrict)

if [[ "${paranoid}" -ne -1 ]] || [[ "${restrict}" -ne 0 ]]; then
    echo "System settings for perf are not configured correctly."
    echo "Please run the following commands"
    echo "echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid"
    echo "echo 0 | sudo tee /proc/sys/kernel/kptr_restrict"
    exit 1
fi

# Check for required tools
for tool in addr2line flamegraph perf; do
    if ! command -v "${tool}" &> /dev/null; then
        echo "Required tool ${tool} is not installed."
        exit 1
    fi
done

# Validate addr2line version
addr2line_version=$(addr2line --version)
if [[ ! "${addr2line_version}" == *"0.21.0"* ]]; then
    echo "Incompatible addr2line version. Please install the correct version."
    exit 1
fi

# Export environment variable for Cargo
export CARGO_PROFILE_RELEASE_DEBUG=true

# Build the project
echo "Building project..."
cargo build --release > /dev/null

# Remove old data
echo "Cleaning old local_data..."
rm -rf local_data perf*

# Variables for log and output files
GIT_INFO=$(get_git_info)
SERVER_LOG_FILE="profiling_server_${GIT_INFO}.log"
BENCH_SEND_LOG_FILE="profiling_bench_send_${GIT_INFO}.log"
BENCH_POLL_LOG_FILE="profiling_bench_poll_${GIT_INFO}.log"
FLAMEGRAPH_SEND_SVG="flamegraph_send_${PROFILED_APP_NAME}_${GIT_INFO}.svg"
FLAMEGRAPH_POLL_SVG="flamegraph_poll_${PROFILED_APP_NAME}_${GIT_INFO}.svg"

# Start iggy-server and capture its PID
echo "Running iggy-server, log will be in ${SERVER_LOG_FILE}..."
target/release/iggy-server > "${SERVER_LOG_FILE}" 2>&1 &
sleep 1

# Run iggy-bench send tcp
echo "Running iggy-bench send tcp..."
if [[ "${PROFILED_APP_NAME}" == "iggy-server" ]]; then
    # Start flamegraph for iggy-server (send)
    echo "Starting flamegraph (send) on iggy-server..."
    flamegraph -o "${FLAMEGRAPH_SEND_SVG}" --pid "$(pgrep iggy-server)" 2>&1 &
    sleep 1
    target/release/iggy-bench send tcp > "${BENCH_SEND_LOG_FILE}"

    # Trigger flamegraph (send) completion
    send_signal "perf" "TERM"
    wait_for_process "perf" 10


    # Start flamegraph for iggy-server (poll)
    echo "Starting flamegraph (poll)..."
    flamegraph -o "${FLAMEGRAPH_POLL_SVG}" --pid "$(pgrep iggy-server)" 2>&1 &
    sleep 1

    # Run iggy-bench poll tcp
    echo "Running iggy-bench poll tcp..."
    target/release/iggy-bench poll tcp > "${BENCH_POLL_LOG_FILE}"

    # Trigger flamegraph (poll) completion
    send_signal "perf" "TERM"

    wait_for_process "perf" 10
else
    echo "Starting flamegraph (send) on iggy-bench..."
    cargo flamegraph --bin iggy-bench -o "${FLAMEGRAPH_SEND_SVG}" -- send tcp > "${BENCH_SEND_LOG_FILE}"
    sleep 1
    echo "Starting flamegraph (poll) on iggy-bench..."
    cargo flamegraph --bin iggy-bench -o "${FLAMEGRAPH_POLL_SVG}" -- poll tcp > "${BENCH_POLL_LOG_FILE}"
fi

# Gracefully stop the server
send_signal "iggy-server" "TERM"
wait_for_process "iggy-server" 10

# Display results
echo
echo "Send results:"
grep -e "Results: total throughput" "${BENCH_SEND_LOG_FILE}"

echo
echo "Poll results:"
grep -e "Results: total throughput" "${BENCH_POLL_LOG_FILE}"

echo
echo "Flamegraph svg (send) saved to ${FLAMEGRAPH_SEND_SVG}"
echo "Flamegraph svg (poll) saved to ${FLAMEGRAPH_POLL_SVG}"

exit 0
