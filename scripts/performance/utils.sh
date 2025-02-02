#!/bin/bash

# Example bench command:
# target/release/iggy-bench --message-size 1000 --messages-per-batch 1000 --message-batches 1000 pinned-producer --streams 8 --producers 8 tcp output --remark "test"

# Function to get the current git tag containing "server" or commit SHA1
function get_git_iggy_server_tag_or_sha1() {
    local dir="$1"

    if [ -d "$dir" ]; then
        pushd "$dir" > /dev/null || {
            echo "Error: Failed to enter directory '$dir'." >&2
            exit 1
        }

        if git rev-parse --git-dir > /dev/null 2>&1; then
            # Get the short commit hash
            local commit_hash
            commit_hash=$(git rev-parse --short HEAD)

            # Get all tags pointing to HEAD that contain "server" (case-insensitive)
            local matching_tags
            matching_tags=$(git tag --points-at HEAD | grep -i "server" || true)

            popd > /dev/null || {
                echo "Error: Failed to return from directory '$dir'." >&2
                exit 1
            }

            if [ -n "$matching_tags" ]; then
                # If multiple tags match, you can choose to return the first one
                # or handle them as needed. Here, we'll return the first match.
                local first_matching_tag
                first_matching_tag=$(echo "$matching_tags" | head -n 1)
                echo "$first_matching_tag"
            else
                # No matching tag found; return the commit hash
                echo "$commit_hash"
            fi
        else
            echo "Error: Directory '$dir' is not a git repository." >&2
            popd > /dev/null || exit 1
            return 1
        fi
    else
        echo "Error: Directory '$dir' does not exist." >&2
        return 1
    fi
}

# Function to get the commit date (last modified date) for HEAD
function get_git_commit_date() {
    local dir="$1"

    if [ -d "$dir" ]; then
        pushd "$dir" > /dev/null || {
            echo "Error: Failed to enter directory '$dir'." >&2
            exit 1
        }

        if git rev-parse --git-dir > /dev/null 2>&1; then
            # Get the committer date (last modified) in ISO 8601 format
            local commit_date
            commit_date=$(git show -s --format=%cI HEAD 2>/dev/null || echo "")

            popd > /dev/null || {
                echo "Error: Failed to return from directory '$dir'." >&2
                exit 1
            }

            if [ -n "$commit_date" ]; then
                echo "$commit_date"
            else
                echo "Error: Could not get commit date for HEAD." >&2
                return 1
            fi
        else
            echo "Error: Directory '$dir' is not a git repository." >&2
            popd > /dev/null || exit 1
            return 1
        fi
    else
        echo "Error: Directory '$dir' does not exist." >&2
        return 1
    fi
}

# Function to construct a bench command
function construct_bench_command() {
    local bench_command=$1
    local benchmark_mode=$2
    local streams=$3
    local actors=$4
    local message_size=$5
    local messages_per_batch=$6
    local message_batches=$7
    local protocol=$8
    local remark=${9:-""}
    local identifier=${10:-$(hostname)}

    # Set variables based on benchmark mode
    local actor_args=""
    case "$benchmark_mode" in
        "pinned-producer")
            actor_args="--producers ${actors}"
            ;;
        "pinned-consumer")
            actor_args="--consumers ${actors}"
            ;;
        "pinned-producer-and-consumer")
            actor_args="--producers ${actors} --consumers ${actors}"
            ;;
        "balanced-producer")
            actor_args="--producers ${actors}"
            ;;
        "balanced-consumer-group")
            actor_args="--consumers ${actors}"
            ;;
        "balanced-producer-and-consumer-group")
            actor_args="--producers ${actors} --consumers ${actors}"
            ;;
        "end-to-end-producing-consumer")
            actor_args="--producers ${actors}"
            ;;
        *)
            echo "Error: Invalid benchmark mode '$benchmark_mode'." >&2
            return 1
            ;;
    esac

    local commit_hash
    commit_hash=$(get_git_iggy_server_tag_or_sha1 .) || { echo "Failed to get git commit or tag."; exit 1; }
    local commit_date
    commit_date=$(get_git_commit_date .) || { echo "Failed to get git commit date."; exit 1; }

    echo "$bench_command \
--message-size ${message_size} \
--messages-per-batch ${messages_per_batch} \
--message-batches ${message_batches} \
${benchmark_mode} \
--streams ${streams} \
${actor_args} \
${protocol} \
output \
--output-dir performance_results \
--identifier ${identifier} \
--remark ${remark} \
--extra-info \"\" \
--gitref \"${commit_hash}\" \
--gitref-date \"${commit_date}\""
}
