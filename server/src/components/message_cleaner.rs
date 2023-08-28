use crate::server_config::MessageCleanerConfig;
use iggy::error::Error;
use iggy::utils::timestamp;
use std::sync::Arc;
use std::time::Duration;
use streaming::systems::system::System;
use streaming::topics::topic::Topic;
use tokio::sync::RwLock;
use tokio::{task, time};
use tracing::{error, info};

pub fn start(config: MessageCleanerConfig, system: Arc<RwLock<System>>) {
    if !config.enabled {
        info!("Message cleaner is disabled.");
        return;
    }

    if config.interval == 0 {
        panic!("Message cleaner interval must be greater than 0.")
    }

    let duration = Duration::from_secs(config.interval);
    task::spawn(async move {
        let mut interval = time::interval(duration);
        info!(
            "Message cleaner is enabled, expired messages will be deleted every: {:?}.",
            duration
        );
        interval.tick().await;
        loop {
            interval.tick().await;
            let system = system.read().await;
            let now = timestamp::get();
            let streams = system.get_streams();
            for stream in streams {
                let topics = stream.get_topics();
                for topic in topics {
                    if delete_expired_segments(topic, now).await.is_err() {
                        error!(
                            "Failed to delete expired segments for stream ID: {}, topic ID: {}",
                            topic.stream_id, topic.topic_id
                        );
                    }
                }
            }
        }
    });
}

async fn delete_expired_segments(topic: &Topic, now: u64) -> Result<(), Error> {
    let expired_segments = topic
        .get_expired_segments_start_offsets_per_partition(now)
        .await;
    if expired_segments.is_empty() {
        info!(
            "No expired segments found for stream ID: {}, topic ID: {}",
            topic.stream_id, topic.topic_id
        );
        return Ok(());
    }

    info!(
        "Found {} expired segments for stream ID: {}, topic ID: {}, deleting...",
        expired_segments.len(),
        topic.stream_id,
        topic.topic_id
    );

    for (partition_id, start_offsets) in &expired_segments {
        let partition = topic.get_partition(*partition_id);
        if partition.is_err() {
            error!(
                "Partition with ID: {} not found for stream ID: {}, topic ID: {}",
                partition_id, topic.stream_id, topic.topic_id
            );
            continue;
        }
        let partition = partition.unwrap();
        let mut partition = partition.write().await;
        let mut last_end_offset = 0;
        for start_offset in start_offsets {
            let deleted_segment = partition.delete_segment(*start_offset).await?;
            last_end_offset = deleted_segment.end_offset;
        }

        if partition.get_segments().is_empty() {
            let start_offset = last_end_offset + 1;
            partition.add_persisted_segment(start_offset).await?;
        }
    }

    Ok(())
}
