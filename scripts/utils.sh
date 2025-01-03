#!/bin/bash

# Function to get the git branch and commit hash
function get_git_info() {
    local git_info
    git_info=$(git log -1 --pretty=format:"%h")
    local git_branch
    git_branch=$(git rev-parse --abbrev-ref HEAD)
    echo "${git_branch}_${git_info}"
}

# OS detection
OS="$(uname -s)"

# Function to check if process exists - OS specific implementations
function process_exists() {
    local pid=$1
    if [ "$OS" = "Darwin" ]; then
        ps -p "${pid}" > /dev/null
    else
        [[ -e /proc/${pid} ]]
    fi
}

# Function to send signal to process - OS specific implementations
function send_signal_to_pid() {
    local pid=$1
    local signal=$2
    if [ "$OS" = "Darwin" ]; then
        kill "-${signal}" "${pid}"
    else
        kill -s "${signal}" "${pid}"
    fi
}

# Function to wait for a process to exit
function wait_for_process() {
    local process_name=$1
    local timeout=$2
    local start_time
    start_time=$(date +%s)
    local end_time=$((start_time + timeout))
    local continue_outer_loop=false

    while [[ $(date +%s) -lt ${end_time} ]]; do
        continue_outer_loop=false
        pids=$(pgrep "${process_name}") || true
        if [[ -n "${pids}" ]]; then
            for pid in ${pids}; do
                if process_exists "${pid}"; then
                    sleep 0.1
                    continue_outer_loop=true
                    break
                fi
            done
            [[ $continue_outer_loop == true ]] && continue
        fi
        return 0
    done

    echo "Timeout waiting for process ${process_name} to exit."
    return 1
}

# Function to send a signal to a process
function send_signal() {
    local process_name=$1
    local pids
    pids=$(pgrep "${process_name}") || true
    local signal=$2

    if [[ -n "${pids}" ]]; then
        for pid in ${pids}; do
            if process_exists "${pid}"; then
                send_signal_to_pid "${pid}" "${signal}"
            fi
        done
    fi
}

# Function to exit with error if a process with the given PID is running
exit_if_process_is_not_running() {
    local pid="$1"

    if kill -0 "$pid" 2>/dev/null; then
        return 0  # Process is running
    else
        exit 1  # Process is not running
    fi
}

# Exit hook for profile.sh
function on_exit_profile() {
    # Gracefully stop the server
    send_signal "iggy-server" "KILL"
    send_signal "iggy-bench" "KILL"
    send_signal "flamegraph" "KILL"
    send_signal "perf" "KILL"
}

# Exit hook for run-benches.sh
function on_exit_bench() {
    send_signal "iggy-server" "KILL"
    send_signal "iggy-bench" "KILL"
}
