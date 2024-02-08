#!/bin/bash

# Function to get the git branch and commit hash
function get_git_info() {
    local git_info
    git_info=$(git log -1 --pretty=format:"%h")
    local git_branch
    git_branch=$(git rev-parse --abbrev-ref HEAD)
    echo "${git_branch}_${git_info}"
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
                if [[ -e /proc/${pid} ]]; then
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
            if [[ -e /proc/${pid} ]]; then
                kill -s "${signal}" "${pid}"
            fi
        done
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
