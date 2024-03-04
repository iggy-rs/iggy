#!/usr/bin/env bash

# This script is used to generate Cross.toml file for user which executes
# this script. This is needed since Cross.toml build.dockerfile.build-args
# section requires statically defined Docker build arguments and parameters
# like current UID or GID must be entered (cannot be generated or fetched
# during cross execution time).

readonly CROSS_TOML_FILE="Cross.toml"
USER_UID=$(id -u)
readonly USER_UID
USER_GID=$(id -g)
readonly USER_GID
USER_NAME=$(id -un)
readonly USER_NAME

echo "Preparing ${CROSS_TOML_FILE} file for user ${USER_NAME} with UID ${USER_UID} and GID ${USER_GID}."

cat << EOF > "${CROSS_TOML_FILE}"
[build.env]
passthrough = ["IGGY_SYSTEM_PATH", "IGGY_CI_BUILD", "RUST_BACKTRACE=1"]

[build.dockerfile]
file = "Dockerfile.cross"
build-args = { USER = "${USER_NAME}", CROSS_CONTAINER_UID = "${USER_UID}", CROSS_CONTAINER_GID = "${USER_GID}" }

EOF
