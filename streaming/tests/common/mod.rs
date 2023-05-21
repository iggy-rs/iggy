use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use tokio::fs;
use streaming::config::SystemConfig;

static DIRECTORY_ID: AtomicUsize = AtomicUsize::new(1);

pub struct TestSetup {
    pub path: String,
    pub config: Arc<SystemConfig>
}

impl TestSetup {
    pub async fn init() -> TestSetup {
        let directory_id = DIRECTORY_ID.fetch_add(1, SeqCst);
        let path = format!("test_data_{}", directory_id);
        let config = Arc::new(SystemConfig {
            path: format!("{}/local_data", path),
            ..Default::default()
        });
        let test_setup = TestSetup {
            path,
            config
        };

        fs::create_dir(&test_setup.path).await.unwrap();

        test_setup
    }
}

impl Drop for TestSetup {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}