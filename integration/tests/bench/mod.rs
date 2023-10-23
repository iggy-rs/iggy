mod http;
mod quic;
mod tcp;

use assert_cmd::prelude::CommandCargoExt;
use std::process::Command;

pub enum Protocol {
    Http,
    Quic,
    Tcp,
}

fn run_bench_and_wait_for_finish(server_addr: &str, protocol: Protocol) {
    let mut command = Command::cargo_bin("iggy-bench").unwrap();

    // When running action from github CI, binary needs to be started via QEMU.
    if let Ok(runner) = std::env::var("QEMU_RUNNER") {
        let mut runner_command = Command::new(runner);
        runner_command.arg(command.get_program().to_str().unwrap());
        command = runner_command;
    };

    let (protocol, protocol_named_arg) = match protocol {
        Protocol::Http => ("--http", "--http-api-url"),
        Protocol::Quic => ("--quic", "--quic-server-address"),
        Protocol::Tcp => ("--tcp", "--tcp-server-address"),
    };

    command.args([
        protocol,
        protocol_named_arg,
        &server_addr,
        "--test-send-messages",
        "--streams",
        "10",
        "--producers",
        "10",
        "--parallel-producer-streams",
        "--messages-per-batch",
        "100",
        "--message-batches",
        "100",
        "--message-size",
        "1000",
    ]);

    let mut child = command.spawn().unwrap();
    let result = child.wait().unwrap();

    assert!(result.success());
}
