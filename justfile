# Convinience commands for iggy
# See https://github.com/casey/just
#
# Usage: just <command>
#
# Commands:

alias b  := build
alias t  := test
alias c  := tests
alias n  := nextest
alias s  := nextests
alias rb := run-benches
alias ps := profile-server
alias pc := profile-client

build:
  cargo build

test: build
  cargo test

tests TEST: build
  cargo test {{TEST}}

nextest: build
  cargo nextest run

nextests TEST: build
  cargo nextest run --nocapture -- {{TEST}}

server:
  cargo run --bin iggy-server

build-tokio-console:
  RUSTFLAGS="--cfg tokio_unstable" cargo build --release --features tokio-console

run-benches:
  ./scripts/run-benches.sh

profile-server:
  ./scripts/profile.sh iggy-server

profile-client:
  ./scripts/profile.sh iggy-bench
