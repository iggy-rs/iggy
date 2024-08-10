use server::configs::system::SystemConfig;
use server::streaming::persistence::persister::FilePersister;
use server::streaming::storage::SystemStorage;
use std::sync::Arc;
use tokio::fs;
use uuid::Uuid;

pub struct TestSetup {
    pub config: Arc<SystemConfig>,
    pub storage: Arc<SystemStorage>,
}

impl TestSetup {
    pub async fn init() -> TestSetup {
        Self::init_with_config(SystemConfig::default()).await
    }

    pub async fn init_with_config(mut config: SystemConfig) -> TestSetup {
        config.path = format!("local_data_{}", Uuid::now_v7().to_u128_le());

        let config = Arc::new(config);
        fs::create_dir(config.get_system_path()).await.unwrap();
        let persister = FilePersister {};
        let storage = Arc::new(SystemStorage::new(config.clone(), Arc::new(persister)));
        TestSetup { config, storage }
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
