use crate::partitions::partition::Partition;
use crate::topics::topic::Topic;
use shared::error::Error;
use std::path::Path;
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::info;

impl Topic {
    pub async fn load(&mut self) -> Result<(), Error> {
        info!("Loading topic with ID: {} from disk...", &self.id);
        if !Path::new(&self.path).exists() {
            return Err(Error::TopicNotFound(self.id));
        }

        let topic_info_file = OpenOptions::new().read(true).open(&self.info_path).await;
        if topic_info_file.is_err() {
            return Err(Error::CannotOpenTopicInfo(self.id));
        }

        let mut topic_info = String::new();
        if topic_info_file
            .unwrap()
            .read_to_string(&mut topic_info)
            .await
            .is_err()
        {
            return Err(Error::CannotReadTopicInfo(self.id));
        }

        self.name = topic_info;
        let dir_files = fs::read_dir(&self.path).await;
        if dir_files.is_err() {
            return Err(Error::CannotReadPartitions(self.id));
        }

        let mut dir_files = dir_files.unwrap();
        loop {
            let dir_entry = dir_files.next_entry().await;
            if dir_entry.is_err() {
                break;
            }

            let dir_entry = dir_entry.unwrap();
            if dir_entry.is_none() {
                break;
            }

            let dir_entry = dir_entry.unwrap();
            let metadata = dir_entry.metadata().await;
            if metadata.is_err() || metadata.unwrap().is_file() {
                continue;
            }

            let id = dir_entry
                .file_name()
                .to_str()
                .unwrap()
                .parse::<u32>()
                .unwrap();
            let mut partition =
                Partition::create(id, &self.path, false, self.config.partition.clone());
            partition.load().await?;
            self.partitions.insert(partition.id, partition);
        }

        // TODO: After loading all the partitions, cache the messages from latest segment(s) based on the available buffer size.

        info!(
            "Loaded topic: '{}' with ID: {} from disk.",
            &self.name, &self.id
        );

        Ok(())
    }

    pub async fn persist(&mut self) -> Result<(), Error> {
        if Path::new(&self.path).exists() {
            return Err(Error::TopicAlreadyExists(self.id));
        }

        if std::fs::create_dir(&self.path).is_err() {
            return Err(Error::CannotCreateTopicDirectory(self.id));
        }

        info!("Topic with ID {} was saved, path: {}", self.id, self.path);

        let topic_info_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.info_path)
            .await;

        if topic_info_file.is_err() {
            return Err(Error::CannotCreateTopicInfo(self.id));
        }

        if topic_info_file
            .unwrap()
            .write_all(self.name.as_bytes())
            .await
            .is_err()
        {
            return Err(Error::CannotUpdateTopicInfo(self.id));
        }

        info!(
            "Creating {} partition(s) for topic with ID: {}...",
            self.partitions.len(),
            &self.id
        );
        for partition in self.partitions.iter_mut() {
            if std::fs::create_dir(&partition.1.path).is_err() {
                return Err(Error::CannotCreatePartitionDirectory(*partition.0, self.id));
            }

            partition.1.persist().await?;
            info!(
                "Partition with ID {} for topic with ID: {} was saved, path: {}",
                partition.0, &self.id, partition.1.path
            );
        }

        Ok(())
    }

    pub async fn persist_messages(&mut self) -> Result<(), Error> {
        for partition in self.get_partitions_mut() {
            for segment in partition.get_segments_mut() {
                segment.persist_messages().await?;
            }
        }

        Ok(())
    }

    pub async fn delete(&self) -> Result<(), Error> {
        info!("Deleting topic with ID: {}...", &self.id);
        if fs::remove_dir_all(&self.path).await.is_err() {
            return Err(Error::CannotDeleteTopicDirectory(self.id));
        }

        info!("Deleted topic with ID: {}.", &self.id);

        Ok(())
    }
}
