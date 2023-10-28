use crate::streaming::storage::{Storage, StreamStorage};
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::topic::Topic;
use anyhow::Context;
use async_trait::async_trait;
use futures::future::join_all;
use iggy::error::Error;
use serde::{Deserialize, Serialize};
use sled::Db;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::create_dir;
use tokio::sync::Mutex;
use tracing::{error, info};

#[derive(Debug)]
pub struct FileStreamStorage {
    db: Arc<Db>,
}

impl FileStreamStorage {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

unsafe impl Send for FileStreamStorage {}
unsafe impl Sync for FileStreamStorage {}

impl StreamStorage for FileStreamStorage {}

#[derive(Debug, Serialize, Deserialize)]
struct StreamData {
    name: String,
    created_at: u64,
}

#[async_trait]
impl Storage<Stream> for FileStreamStorage {
    async fn load(&self, stream: &mut Stream) -> Result<(), Error> {
        info!("Loading stream with ID: {} from disk...", stream.stream_id);
        if !Path::new(&stream.path).exists() {
            return Err(Error::StreamIdNotFound(stream.stream_id));
        }

        let key = get_key(stream.stream_id);
        let stream_data = match self.db.get(&key).with_context(|| {
            format!(
                "Failed to load stream with ID: {}, key: {}",
                stream.stream_id, key
            )
        }) {
            Ok(stream_data) => {
                if let Some(stream_data) = stream_data {
                    let stream_data = rmp_serde::from_slice::<StreamData>(&stream_data)
                        .with_context(|| {
                            format!(
                                "Failed to deserialize stream with ID: {}, key: {}",
                                stream.stream_id, key
                            )
                        });
                    match stream_data {
                        Ok(stream_data) => stream_data,
                        Err(err) => {
                            return Err(Error::CannotDeserializeResource(err));
                        }
                    }
                } else {
                    return Err(Error::ResourceNotFound(key));
                }
            }
            Err(err) => {
                return Err(Error::CannotLoadResource(err));
            }
        };

        stream.name = stream_data.name;
        stream.created_at = stream_data.created_at;
        let mut unloaded_topics = Vec::new();
        let dir_entries = fs::read_dir(&stream.topics_path).await;
        if dir_entries.is_err() {
            return Err(Error::CannotReadTopics(stream.stream_id));
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
                stream.stream_id,
                topic_id,
                stream.config.clone(),
                stream.storage.clone(),
            );
            unloaded_topics.push(topic);
        }

        let loaded_topics = Arc::new(Mutex::new(Vec::new()));
        let mut load_topics = Vec::new();
        for mut topic in unloaded_topics {
            let loaded_topics = loaded_topics.clone();
            let load_stream = tokio::spawn(async move {
                match topic.load().await {
                    Ok(_) => loaded_topics.lock().await.push(topic),
                    Err(error) => error!(
                        "Failed to load topic with ID: {} for stream with ID: {}. Error: {}",
                        topic.topic_id, topic.stream_id, error
                    ),
                }
            });
            load_topics.push(load_stream);
        }

        join_all(load_topics).await;
        for topic in loaded_topics.lock().await.drain(..) {
            if stream.topics.contains_key(&topic.topic_id) {
                error!(
                    "Topic with ID: '{}' already exists for stream with ID: {}.",
                    &topic.topic_id, &stream.stream_id
                );
                continue;
            }

            if stream.topics_ids.contains_key(&topic.name) {
                error!(
                    "Topic with name: '{}' already exists for stream with ID: {}.",
                    &topic.name, &stream.stream_id
                );
                continue;
            }

            stream.topics_ids.insert(topic.name.clone(), topic.topic_id);
            stream.topics.insert(topic.topic_id, topic);
        }

        info!(
            "Loaded stream: '{}' with ID: {} from disk.",
            &stream.name, &stream.stream_id
        );

        Ok(())
    }

    async fn save(&self, stream: &Stream) -> Result<(), Error> {
        if !Path::new(&stream.path).exists() && create_dir(&stream.path).await.is_err() {
            return Err(Error::CannotCreateStreamDirectory(
                stream.stream_id,
                stream.path.clone(),
            ));
        }

        if !Path::new(&stream.topics_path).exists()
            && create_dir(&stream.topics_path).await.is_err()
        {
            return Err(Error::CannotCreateTopicsDirectory(
                stream.stream_id,
                stream.topics_path.clone(),
            ));
        }

        let key = get_key(stream.stream_id);
        match rmp_serde::to_vec(&StreamData {
            name: stream.name.clone(),
            created_at: stream.created_at,
        })
        .with_context(|| format!("Failed to serialize stream with key: {}", key))
        {
            Ok(data) => {
                if let Err(err) = self
                    .db
                    .insert(&key, data)
                    .with_context(|| format!("Failed to insert stream with key: {}", key))
                {
                    return Err(Error::CannotSaveResource(err));
                }
            }
            Err(err) => {
                return Err(Error::CannotSerializeResource(err));
            }
        }

        info!("Saved stream with ID: {}.", stream.stream_id);

        Ok(())
    }

    async fn delete(&self, stream: &Stream) -> Result<(), Error> {
        info!("Deleting stream with ID: {}...", stream.stream_id);
        let key = get_key(stream.stream_id);
        if let Err(err) = self
            .db
            .remove(&key)
            .with_context(|| format!("Failed to delete stream with key: {}", key))
        {
            return Err(Error::CannotDeleteResource(err));
        }
        if fs::remove_dir_all(&stream.path).await.is_err() {
            return Err(Error::CannotDeleteStreamDirectory(stream.stream_id));
        }
        info!("Deleted stream with ID: {}.", stream.stream_id);
        Ok(())
    }
}

fn get_key(stream_id: u32) -> String {
    format!("streams:{}", stream_id)
}
