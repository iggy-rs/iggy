use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::topic::Topic;
use anyhow::Context;
use iggy::error::IggyError;
use iggy::utils::timestamp::IggyTimestamp;
use serde::{Deserialize, Serialize};
use sled::Db;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::create_dir;
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

#[derive(Debug, Serialize, Deserialize)]
struct StreamData {
    name: String,
    created_at: IggyTimestamp,
}

impl FileStreamStorage {
    async fn load(&self, stream: &mut Stream) -> Result<(), IggyError> {
        info!("Loading stream with ID: {} from disk...", stream.stream_id);
        if !Path::new(&stream.path).exists() {
            return Err(IggyError::StreamIdNotFound(stream.stream_id));
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
                            return Err(IggyError::CannotDeserializeResource(err));
                        }
                    }
                } else {
                    return Err(IggyError::ResourceNotFound(key));
                }
            }
            Err(err) => {
                return Err(IggyError::CannotLoadResource(err));
            }
        };

        stream.name = stream_data.name;
        stream.created_at = stream_data.created_at;
        let mut topics = Vec::new();
        let dir_entries = fs::read_dir(&stream.topics_path).await;
        if dir_entries.is_err() {
            return Err(IggyError::CannotReadTopics(stream.stream_id));
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
                "topic", // TODO: Load name
                stream.size_bytes.clone(),
                stream.messages_count.clone(),
                stream.segments_count.clone(),
                stream.config.clone(),
                stream.storage.clone(),
            );
            topics.push(topic);
        }

        // TODO: Load topics from DB
        for topic in topics.into_iter() {
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

    async fn save(&self, stream: &Stream) -> Result<(), IggyError> {
        if !Path::new(&stream.path).exists() && create_dir(&stream.path).await.is_err() {
            return Err(IggyError::CannotCreateStreamDirectory(
                stream.stream_id,
                stream.path.clone(),
            ));
        }

        if !Path::new(&stream.topics_path).exists()
            && create_dir(&stream.topics_path).await.is_err()
        {
            return Err(IggyError::CannotCreateTopicsDirectory(
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
                    return Err(IggyError::CannotSaveResource(err));
                }
            }
            Err(err) => {
                return Err(IggyError::CannotSerializeResource(err));
            }
        }

        info!("Saved stream with ID: {}.", stream.stream_id);

        Ok(())
    }
}

fn get_key(stream_id: u32) -> String {
    format!("streams:{}", stream_id)
}
