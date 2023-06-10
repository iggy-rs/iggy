use crate::streams::stream::Stream;
use crate::topics::topic::Topic;
use futures::future::join_all;
use shared::error::Error;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::{create_dir, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::{error, info};

impl Stream {
    pub async fn persist(&self) -> Result<(), Error> {
        if Path::new(&self.path).exists() {
            return Err(Error::StreamAlreadyExists(self.id));
        }

        if !Path::new(&self.path).exists() && create_dir(&self.path).await.is_err() {
            return Err(Error::CannotCreateStreamDirectory(self.id));
        }

        if !Path::new(&self.topics_path).exists() && create_dir(&self.topics_path).await.is_err() {
            return Err(Error::CannotCreateTopicsDirectory(self.id));
        }

        let stream_info_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.info_path)
            .await;

        if stream_info_file.is_err() {
            return Err(Error::CannotCreateStreamInfo(self.id));
        }

        if stream_info_file
            .unwrap()
            .write_all(self.name.as_bytes())
            .await
            .is_err()
        {
            return Err(Error::CannotUpdateStreamInfo(self.id));
        }

        Ok(())
    }

    pub async fn persist_messages(&self, enforce_sync: bool) -> Result<(), Error> {
        for topic in self.get_topics() {
            topic.persist_messages(enforce_sync).await?;
        }

        Ok(())
    }

    pub async fn load(&mut self) -> Result<(), Error> {
        info!("Loading stream with ID: {} from disk...", self.id);
        if !Path::new(&self.path).exists() {
            return Err(Error::StreamNotFound(self.id));
        }

        let stream_info_file = OpenOptions::new().read(true).open(&self.info_path).await;
        if stream_info_file.is_err() {
            return Err(Error::CannotOpenStreamInfo(self.id));
        }

        let mut stream_info = String::new();
        if stream_info_file
            .unwrap()
            .read_to_string(&mut stream_info)
            .await
            .is_err()
        {
            return Err(Error::CannotReadStreamInfo(self.id));
        }

        self.name = stream_info;
        let mut unloaded_topics = Vec::new();
        let dir_entries = fs::read_dir(&self.topics_path).await;
        if dir_entries.is_err() {
            return Err(Error::CannotReadTopics(self.id));
        }

        let mut dir_entries = dir_entries.unwrap();
        while let Some(dir_entry) = dir_entries.next_entry().await.unwrap_or(None) {
            let name = dir_entry.file_name().into_string().unwrap();
            let topic_id = name.parse::<u32>();
            if topic_id.is_err() {
                error!("Invalid topic ID file with name: '{}'.", name);
                continue;
            }

            let topic_id = topic_id.unwrap();
            let topic = Topic::empty(
                self.id,
                topic_id,
                &self.topics_path,
                self.config.topic.clone(),
            );
            unloaded_topics.push(topic);
        }

        let loaded_topics = Arc::new(Mutex::new(Vec::new()));
        let mut load_topics = Vec::new();
        for mut topic in unloaded_topics {
            let loaded_topics = loaded_topics.clone();
            let load_stream = tokio::spawn(async move {
                if topic.load().await.is_err() {
                    error!(
                        "Failed to load topic with ID: {} for stream with ID: {}.",
                        topic.id, topic.stream_id
                    );
                    return;
                }

                loaded_topics.lock().await.push(topic);
            });
            load_topics.push(load_stream);
        }

        join_all(load_topics).await;
        for topic in loaded_topics.lock().await.drain(..) {
            self.topics.insert(topic.id, topic);
        }

        info!(
            "Loaded stream: '{}' with ID: {} from disk.",
            &self.name, &self.id
        );

        Ok(())
    }

    pub async fn delete(&self) -> Result<(), Error> {
        info!("Deleting stream with ID: {}...", self.id);
        if fs::remove_dir_all(&self.path).await.is_err() {
            return Err(Error::CannotDeleteStreamDirectory(self.id));
        }
        info!("Deleted stream with ID: {}.", self.id);

        Ok(())
    }
}
