use crate::state::system::StreamState;
use crate::streaming::storage::StreamStorage;
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::topic::Topic;
use futures::{future::join_all, lock::Mutex};
use iggy::error::IggyError;
use iggy::utils::timestamp::IggyTimestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use tracing::{error, info, warn};

#[derive(Debug)]
pub struct FileStreamStorage {
    is_enabled: bool,
}

impl FileStreamStorage {
    pub fn new() -> Self {
        Self { is_enabled: true }
    }
    pub fn noop() -> Self {
        Self { is_enabled: false }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct StreamData {
    name: String,
    created_at: IggyTimestamp,
}

impl StreamStorage for FileStreamStorage {
    async fn load(&self, stream: &mut Stream, mut state: StreamState) -> Result<(), IggyError> {
        if self.is_enabled {
            info!("Loading stream with ID: {} from disk...", stream.stream_id);
            if !Path::new(&stream.path).exists() {
                return Err(IggyError::StreamIdNotFound(stream.stream_id));
            }

            let mut unloaded_topics = Vec::new();
            let dir_entries = std::fs::read_dir(&stream.topics_path);
            if dir_entries.is_err() {
                return Err(IggyError::CannotReadTopics(stream.stream_id));
            }

            let mut dir_entries = dir_entries.unwrap();
            while let Some(dir_entry) = dir_entries.next() {
                let dir_entry = dir_entry.unwrap();
                let name = dir_entry.file_name().into_string().unwrap();
                let topic_id = name.parse::<u32>();
                if topic_id.is_err() {
                    error!("Invalid topic ID file with name: '{}'.", name);
                    continue;
                }

                let topic_id = topic_id.unwrap();
                let topic_state = state.topics.get(&topic_id);
                if topic_state.is_none() {
                    let stream_id = stream.stream_id;
                    error!("Topic with ID: '{topic_id}' for stream with ID: '{stream_id}' was not found in state, but exists on disk and will be removed.");
                    if let Err(error) = std::fs::remove_dir_all(&dir_entry.path()) {
                        error!("Cannot remove topic directory: {error}");
                    } else {
                        warn!("Topic with ID: '{topic_id}' for stream with ID: '{stream_id}' was removed.");
                    }
                    continue;
                }

                let topic_state = topic_state.unwrap();
                let topic = Topic::empty(
                    stream.stream_id,
                    topic_id,
                    &topic_state.name,
                    stream.size_bytes.clone(),
                    stream.messages_count.clone(),
                    stream.segments_count.clone(),
                    stream.config.clone(),
                    stream.storage.clone(),
                );
                unloaded_topics.push(topic);
            }

            let state_topic_ids = state.topics.keys().copied().collect::<HashSet<u32>>();
            let unloaded_topic_ids = unloaded_topics
                .iter()
                .map(|topic| topic.topic_id)
                .collect::<HashSet<u32>>();
            let missing_ids = state_topic_ids
                .difference(&unloaded_topic_ids)
                .copied()
                .collect::<HashSet<u32>>();
            if missing_ids.is_empty() {
                info!(
                    "All topics for stream with ID: '{}' found on disk were found in state.",
                    stream.stream_id
                );
            } else {
                error!("Topics with IDs: '{missing_ids:?}' for stream with ID: '{}' were not found on disk.", stream.stream_id);
                return Err(IggyError::MissingTopics(stream.stream_id));
            }

            let loaded_topics = Arc::new(Mutex::new(Vec::new()));
            let mut load_topics = Vec::new();
            for mut topic in unloaded_topics {
                let loaded_topics = loaded_topics.clone();
                let topic_state = state.topics.remove(&topic.topic_id).unwrap();
                let load_topic = monoio::spawn(async move {
                    match topic.load(topic_state).await {
                        Ok(_) => loaded_topics.lock().await.push(topic),
                        Err(error) => error!(
                            "Failed to load topic with ID: {} for stream with ID: {}. Error: {}",
                            topic.topic_id, topic.stream_id, error
                        ),
                    }
                });
                load_topics.push(load_topic);
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
        }

        Ok(())
    }

    async fn save(&self, stream: &Stream) -> Result<(), IggyError> {
        if self.is_enabled {
            if !Path::new(&stream.path).exists() && std::fs::create_dir(&stream.path).is_err() {
                return Err(IggyError::CannotCreateStreamDirectory(
                    stream.stream_id,
                    stream.path.clone(),
                ));
            }

            if !Path::new(&stream.topics_path).exists()
                && std::fs::create_dir(&stream.topics_path).is_err()
            {
                return Err(IggyError::CannotCreateTopicsDirectory(
                    stream.stream_id,
                    stream.topics_path.clone(),
                ));
            }

            info!("Saved stream with ID: {}.", stream.stream_id);
        }
        Ok(())
    }

    async fn delete(&self, stream: &Stream) -> Result<(), IggyError> {
        if self.is_enabled {
            info!("Deleting stream with ID: {}...", stream.stream_id);
            if std::fs::remove_dir_all(&stream.path).is_err() {
                return Err(IggyError::CannotDeleteStreamDirectory(stream.stream_id));
            }
            info!("Deleted stream with ID: {}.", stream.stream_id);
        }
        Ok(())
    }
}
