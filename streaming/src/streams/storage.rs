use crate::persister::Persister;
use crate::storage::{Storage, StreamStorage};
use crate::streams::stream::Stream;
use crate::topics::topic::Topic;
use crate::utils::file;
use async_trait::async_trait;
use futures::future::join_all;
use iggy::error::Error;
use iggy::utils::text;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::create_dir;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;
use tracing::{error, info};

#[derive(Debug)]
pub struct FileStreamStorage {
    persister: Arc<dyn Persister>,
}

impl FileStreamStorage {
    pub fn new(persister: Arc<dyn Persister>) -> Self {
        Self { persister }
    }
}

unsafe impl Send for FileStreamStorage {}
unsafe impl Sync for FileStreamStorage {}

impl StreamStorage for FileStreamStorage {}

#[async_trait]
impl Storage<Stream> for FileStreamStorage {
    async fn load(&self, stream: &mut Stream) -> Result<(), Error> {
        info!("Loading stream with ID: {} from disk...", stream.id);
        if !Path::new(&stream.path).exists() {
            return Err(Error::StreamIdNotFound(stream.id));
        }

        let stream_info_file = file::open(&stream.info_path).await;
        if stream_info_file.is_err() {
            return Err(Error::CannotOpenStreamInfo(stream.id));
        }

        let mut stream_info = String::new();
        if stream_info_file
            .unwrap()
            .read_to_string(&mut stream_info)
            .await
            .is_err()
        {
            return Err(Error::CannotReadStreamInfo(stream.id));
        }

        stream.name = text::to_lowercase_non_whitespace(&stream_info);
        let mut unloaded_topics = Vec::new();
        let dir_entries = fs::read_dir(&stream.topics_path).await;
        if dir_entries.is_err() {
            return Err(Error::CannotReadTopics(stream.id));
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
                stream.id,
                topic_id,
                &stream.topics_path,
                stream.config.topic.clone(),
                stream.storage.clone(),
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
            if stream.topics.contains_key(&topic.id) {
                error!(
                    "Topic with ID: '{}' already exists for stream with ID: {}.",
                    &topic.id, &stream.id
                );
                continue;
            }

            if stream.topics_ids.contains_key(&topic.name) {
                error!(
                    "Topic with name: '{}' already exists for stream with ID: {}.",
                    &topic.name, &stream.id
                );
                continue;
            }

            stream.topics_ids.insert(topic.name.clone(), topic.id);
            stream.topics.insert(topic.id, topic);
        }

        info!(
            "Loaded stream: '{}' with ID: {} from disk.",
            &stream.name, &stream.id
        );

        Ok(())
    }

    async fn save(&self, stream: &Stream) -> Result<(), Error> {
        if Path::new(&stream.path).exists() {
            return Err(Error::StreamIdAlreadyExists(stream.id));
        }

        if !Path::new(&stream.path).exists() && create_dir(&stream.path).await.is_err() {
            return Err(Error::CannotCreateStreamDirectory(stream.id));
        }

        if !Path::new(&stream.topics_path).exists()
            && create_dir(&stream.topics_path).await.is_err()
        {
            return Err(Error::CannotCreateTopicsDirectory(stream.id));
        }

        if self
            .persister
            .overwrite(&stream.info_path, stream.name.as_bytes())
            .await
            .is_err()
        {
            return Err(Error::CannotCreateStreamInfo(stream.id));
        }

        Ok(())
    }

    async fn delete(&self, stream: &Stream) -> Result<(), Error> {
        info!("Deleting stream with ID: {}...", stream.id);
        if fs::remove_dir_all(&stream.path).await.is_err() {
            return Err(Error::CannotDeleteStreamDirectory(stream.id));
        }
        info!("Deleted stream with ID: {}.", stream.id);
        Ok(())
    }
}
