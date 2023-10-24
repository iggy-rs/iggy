mod http;
mod quic;
mod tcp;

use assert_cmd::prelude::CommandCargoExt;
use integration::test_server::{Transport, TEST_VERBOSITY_ENV_VAR};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    process::{Command, Stdio},
    thread::panicking,
};
use uuid::Uuid;

const BENCH_FILES_PREFIX: &str = "bench_";

fn run_bench_and_wait_for_finish(server_addr: &str, transport: Transport) {
    let mut command = Command::cargo_bin("iggy-bench").unwrap();

    // When running action from github CI, binary needs to be started via QEMU.
    if let Ok(runner) = std::env::var("QEMU_RUNNER") {
        let mut runner_command = Command::new(runner);
        runner_command.arg(command.get_program().to_str().unwrap());
        command = runner_command;
    };

    let mut stderr_file_path = None;
    let mut stdout_file_path = None;

    if std::env::var(TEST_VERBOSITY_ENV_VAR).is_err() {
        let stderr_file = get_random_path();
        let stdout_file = get_random_path();
        stderr_file_path = Some(stderr_file);
        stdout_file_path = Some(stdout_file);
    }

    // 10 MB of data written to disk
    command.args([
        "send-and-poll",
        "--messages-per-batch",
        "100",
        "--message-batches",
        "100",
        "--message-size",
        "1000",
        &format!("{}", transport),
        "--server-address",
        &server_addr,
    ]);

    // By default, all iggy-bench logs are redirected to files,
    // and dumped to stderr when test fails. With IGGY_TEST_VERBOSE=1
    // logs are dumped to stdout during test execution.
    if std::env::var(TEST_VERBOSITY_ENV_VAR).is_ok() {
        command.stdout(Stdio::inherit());
        command.stderr(Stdio::inherit());
    } else {
        command.stdout(File::create(stdout_file_path.as_ref().unwrap()).unwrap());
        stdout_file_path = Some(
            fs::canonicalize(stdout_file_path.unwrap())
                .unwrap()
                .display()
                .to_string(),
        );

        command.stderr(File::create(stderr_file_path.as_ref().unwrap()).unwrap());
        stderr_file_path = Some(
            fs::canonicalize(stderr_file_path.unwrap())
                .unwrap()
                .display()
                .to_string(),
        );
    }

    let mut child = command.spawn().unwrap();
    let result = child.wait().unwrap();

    // cleanup

    if let Ok(output) = child.wait_with_output() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        if let Some(stderr_file_path) = &stderr_file_path {
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(stderr_file_path)
                .unwrap()
                .write_all(stderr.as_bytes())
                .unwrap();
        }

        if let Some(stdout_file_path) = &stdout_file_path {
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(stdout_file_path)
                .unwrap()
                .write_all(stdout.as_bytes())
                .unwrap();
        }
    }

    // TODO: fix this, it needs to be called in Drop
    if panicking() {
        if let Some(stdout_file_path) = &stdout_file_path {
            eprintln!(
                "Iggy bench stdout:\n{}",
                fs::read_to_string(stdout_file_path).unwrap()
            );
        }

        if let Some(stderr_file_path) = &stderr_file_path {
            eprintln!(
                "Iggy bench stderr:\n{}",
                fs::read_to_string(stderr_file_path).unwrap()
            );
        }
    }

    if let Some(stdout_file_path) = &stdout_file_path {
        fs::remove_file(stdout_file_path).unwrap();
    }
    if let Some(stderr_file_path) = &stderr_file_path {
        fs::remove_file(stderr_file_path).unwrap();
    }

    assert!(result.success());
}

pub fn get_random_path() -> String {
    format!("{}{}", BENCH_FILES_PREFIX, Uuid::new_v4().to_u128_le())
}
