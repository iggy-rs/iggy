pub mod http;
pub mod quic;
pub mod scenarios;
pub mod tcp;

use assert_cmd::prelude::CommandCargoExt;
use async_trait::async_trait;
use iggy::client::Client;
use std::fs;
use std::process::{Child, Command};
use std::thread::sleep;
use std::time::Duration;
use streaming::utils::random_id;

#[async_trait]
pub trait ClientFactory: Sync + Send {
    async fn create_client(&self) -> Box<dyn Client>;
}

pub struct TestServer {
    files_path: String,
    child_handle: Option<Child>,
}

impl TestServer {
    pub fn new(files_path: String) -> Self {
        Self {
            files_path,
            child_handle: None,
        }
    }

    pub fn start(&mut self) {
        // Sleep before starting server - it takes some time for the OS to release the port
        sleep(Duration::from_secs(3));

        self.cleanup();
        let files_path = self.files_path.clone();
        let mut command = Command::cargo_bin("iggy-server").unwrap();
        command.env("IGGY_SYSTEM_PATH", files_path.clone());

        // When running action from github CI, binary needs to be started via QEMU.
        if let Ok(runner) = std::env::var("QEMU_RUNNER") {
            let mut runner_command = Command::new(runner);
            runner_command
                .arg(command.get_program().to_str().unwrap())
                .env("IGGY_SYSTEM_PATH", files_path);
            command = runner_command;
        };
        self.child_handle = Some(command.spawn().unwrap());

        // Sleep after starting server - it needs some time to bind to given port and start listening
        let sleep_duration = if cfg!(any(
            target = "aarch64-unknown-linux-musl",
            target = "arm-unknown-linux-musleabi"
        )) {
            Duration::from_secs(40)
        } else {
            Duration::from_secs(3)
        };
        sleep(sleep_duration);
    }

    pub fn stop(&mut self) {
        if let Some(mut child_handle) = self.child_handle.take() {
            child_handle.kill().unwrap();
        }
        self.cleanup();
    }

    fn cleanup(&self) {
        if fs::metadata(&self.files_path).is_ok() {
            fs::remove_dir_all(&self.files_path).unwrap();
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Default for TestServer {
    fn default() -> Self {
        TestServer::new(format!("local_data_{}", random_id::get()))
    }
}
