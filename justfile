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
alias pcs := profile-cpu-server
alias pcc := profile-cpu-client
alias pis := profile-io-server
alias pic := profile-io-client

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

profile-cpu-server:
  ./scripts/profile.sh iggy-server cpu

profile-cpu-client:
  ./scripts/profile.sh iggy-bench cpu

profile-io-server:
  ./scripts/profile.sh iggy-server io

profile-io-client:
  ./scripts/profile.sh iggy-bench io
