use sled::Db;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use streaming::config::SystemConfig;
use streaming::persister::FilePersister;
use streaming::storage::SystemStorage;
use tokio::fs;

static DIRECTORY_ID: AtomicUsize = AtomicUsize::new(1);

pub struct TestSetup {
    pub config: Arc<SystemConfig>,
    pub storage: Arc<SystemStorage>,
    pub db: Arc<Db>,
}

#[allow(dead_code)]
impl TestSetup {
    pub async fn init() -> TestSetup {
        let directory_id = DIRECTORY_ID.fetch_add(1, SeqCst);
        let path = format!("test_local_data_{}", directory_id);
        let config = Arc::new(SystemConfig {
            path,
            ..Default::default()
        });
        fs::create_dir(config.get_system_path()).await.unwrap();
        let persister = FilePersister {};
        let db = Arc::new(sled::open(config.get_database_path()).unwrap());
        let storage = Arc::new(SystemStorage::new(db.clone(), Arc::new(persister)));
        TestSetup {
            config,
            storage,
            db,
        }
    }

    pub async fn create_streams_directory(&self) {
        if fs::metadata(&self.config.get_streams_path()).await.is_err() {
            fs::create_dir(&self.config.get_streams_path())
                .await
                .unwrap();
        }
    }

    pub async fn create_stream_directory(&self, stream_id: u32) {
        self.create_streams_directory().await;
        if fs::metadata(&self.config.get_stream_path(stream_id))
            .await
            .is_err()
        {
            fs::create_dir(&self.config.get_stream_path(stream_id))
                .await
                .unwrap();
        }
    }

    pub async fn create_topics_directory(&self, stream_id: u32) {
        self.create_stream_directory(stream_id).await;
        if fs::metadata(&self.config.get_topics_path(stream_id))
            .await
            .is_err()
        {
            fs::create_dir(&self.config.get_topics_path(stream_id))
                .await
                .unwrap();
        }
    }

    pub async fn create_topic_directory(&self, stream_id: u32, topic_id: u32) {
        self.create_topics_directory(stream_id).await;
        if fs::metadata(&self.config.get_topic_path(stream_id, topic_id))
            .await
            .is_err()
        {
            fs::create_dir(&self.config.get_topic_path(stream_id, topic_id))
                .await
                .unwrap();
        }
    }

    pub async fn create_partitions_directory(&self, stream_id: u32, topic_id: u32) {
        self.create_topic_directory(stream_id, topic_id).await;
        if fs::metadata(&self.config.get_partitions_path(stream_id, topic_id))
            .await
            .is_err()
        {
            fs::create_dir(&self.config.get_partitions_path(stream_id, topic_id))
                .await
                .unwrap();
        }
    }

    pub async fn create_partition_directory(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
    ) {
        self.create_partitions_directory(stream_id, topic_id).await;
        if fs::metadata(
            &self
                .config
                .get_partition_path(stream_id, topic_id, partition_id),
        )
        .await
        .is_err()
        {
            fs::create_dir(
                &self
                    .config
                    .get_partition_path(stream_id, topic_id, partition_id),
            )
            .await
            .unwrap();
        }
    }
}

impl Drop for TestSetup {
    fn drop(&mut self) {
        std::fs::remove_dir_all(self.config.get_system_path()).unwrap();
    }
}
