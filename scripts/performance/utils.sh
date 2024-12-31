#!/bin/bash

# Example bench command:
# target/release/iggy-bench send --producers 8 --streams 8 --message-size 1000 --messages-per-batch 1000 --message-batches 1000 tcp

COMMON_ARGS="--warmup-time 0"

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

# Function to construct a bench command (send or poll)
function construct_bench_command() {
    local bench_command=$1
    local type=$2
    local count=$3
    local message_size=$4
    local messages_per_batch=$5
    local message_batches=$6
    local protocol=$7

    # Validate the type
    if [[ "$type" != "send" && "$type" != "poll" ]]; then
        echo "Error: Invalid type '$type'. Must be 'send' or 'poll'." >&2
        return 1
    fi

    # Set variables based on type
    if [[ "$type" == "send" ]]; then
        local role="producers"
    else
        local role="consumers"
    fi

    local streams=${count}

    local superdir
    superdir="performance_results/$(get_git_iggy_server_tag_or_sha1 .)" || { echo "Failed to get git commit or tag."; exit 1; }
    rm -rf "$superdir" || true
    mkdir -p "$superdir" || { echo "Failed to create directory '$superdir'."; exit 1; }
    local output_directory="${superdir}/${type}_${count}${type:0:1}_${message_size}_${messages_per_batch}_${message_batches}_${protocol}"

    echo "$bench_command \
    $COMMON_ARGS \
    --output-directory $output_directory \
    ${type} \
    --${role} ${count} \
    --streams ${streams} \
    --message-size ${message_size} \
    --messages-per-batch ${messages_per_batch} \
    --message-batches ${message_batches} \
    ${protocol}"
}
