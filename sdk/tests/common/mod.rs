use std::fs;
use tokio::process::Command;
use tokio::runtime::Runtime;

pub struct TestServer {
    files_path: String,
    runtime: Runtime,
}

impl TestServer {
    pub fn new(files_path: String) -> Self {
        let runtime = Runtime::new().unwrap();
        Self {
            files_path,
            runtime,
        }
    }

    pub fn start(&self) {
        self.cleanup();
        self.runtime.spawn(async {
            Command::new("cargo")
                .kill_on_drop(true)
                .args(&["r", "--manifest-path", "../server/Cargo.toml"])
                .spawn()
                .expect("Could not start server")
                .wait()
                .await
                .unwrap()
        });
    }

    pub fn stop(self) {
        self.cleanup();
        self.runtime.shutdown_background();
    }

    fn cleanup(&self) {
        if fs::metadata(&self.files_path).is_ok() {
            fs::remove_dir_all(&self.files_path).unwrap();
        }
    }
}

impl Default for TestServer {
    fn default() -> Self {
        TestServer::new("local_data".to_string())
    }
}
