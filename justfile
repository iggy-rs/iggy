# Convinience commands for iggy
# See https://github.com/casey/just
#
# Usage: just <command>
#
# Commands:

test:
  cargo build && cargo test

server:
  cargo run --bin iggy-server
