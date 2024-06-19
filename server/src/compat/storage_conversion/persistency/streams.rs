use crate::compat::storage_conversion::persistency::topics;
use crate::configs::system::SystemConfig;
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::topic::Topic;
use anyhow::Context;
use iggy::error::IggyError;
use iggy::utils::timestamp::IggyTimestamp;
use serde::{Deserialize, Serialize};
use sled::Db;
use std::path::Path;
use tokio::fs;
use tracing::{error, info};

#[derive(Debug, Serialize, Deserialize)]
struct StreamData {
    name: String,
    created_at: IggyTimestamp,
}

pub async fn load(config: &SystemConfig, db: &Db, stream: &mut Stream) -> Result<(), IggyError> {
    info!("Loading stream with ID: {} from disk...", stream.stream_id);
    if !Path::new(&stream.path).exists() {
        return Err(IggyError::StreamIdNotFound(stream.stream_id));
    }

    let key = get_key(stream.stream_id);
    let stream_data = match db.get(&key).with_context(|| {
        format!(
            "Failed to load stream with ID: {}, key: {}",
            stream.stream_id, key
        )
    }) {
        Ok(stream_data) => {
            if let Some(stream_data) = stream_data {
                let stream_data =
                    rmp_serde::from_slice::<StreamData>(&stream_data).with_context(|| {
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
        let mut topic = Topic::empty(
            stream.stream_id,
            topic_id,
            "topic",
            stream.size_bytes.clone(),
            stream.messages_count.clone(),
            stream.segments_count.clone(),
            stream.config.clone(),
            stream.storage.clone(),
        );
        topics::load(config, db, &mut topic).await?;
        topics.push(topic);
    }

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

fn get_key(stream_id: u32) -> String {
    format!("streams:{}", stream_id)
}
