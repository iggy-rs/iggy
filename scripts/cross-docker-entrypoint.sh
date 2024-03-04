#!/usr/bin/env bash

# Script designed for iggy internal tests for non-native / non-x86_64
# architectures inside Docker container (using cross tool).

set -euo pipefail

echo "Got following docker command \"$*\""

# If this is cargo build command do execute it
if [[ $* == *"cargo test"* ]]; then
    # Strip prefix (sh -c) as it is not evaluated properly under
    # dbus / gnome-keyring session.
    command="$*"
    command=${command#sh -c }

    # Unlock keyring and run command
    dbus-run-session -- sh -c "echo \"iggy\" | gnome-keyring-daemon --unlock && eval \"${command}\""
else
    # Otherwise (non cargo test scenario) just execute what has been requested
    exec "$@"
fi
