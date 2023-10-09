use crate::channels::executor::ExecutableServerCommand;
use crate::streaming::systems::system::System;
use crate::streaming::topics::topic::Topic;
use async_trait::async_trait;
use iggy::error::Error;
use iggy::utils::timestamp::TimeStamp;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

#[derive(Debug, Default, Clone)]
pub struct CleanMessagesCommand;

#[derive(Debug, Default)]
pub struct CleanMessagesExecutor;

#[async_trait]
impl ExecutableServerCommand for CleanMessagesExecutor {
    type Command = CleanMessagesCommand;

    async fn execute(&mut self, system: Arc<RwLock<System>>, _command: CleanMessagesCommand) {
        let now = TimeStamp::now().to_micros();
        let system_read = system.read().await;
        let streams = system_read.get_streams();
        for stream in streams {
            let topics = stream.get_topics();
            for topic in topics {
                let deleted_segments = delete_expired_segments(topic, now).await;
                if let Ok(Some(deleted_segments)) = deleted_segments {
                    info!(
                        "Deleted {} segments and {} messages for stream ID: {}, topic ID: {}",
                        deleted_segments.segments_count,
                        deleted_segments.messages_count,
                        topic.stream_id,
                        topic.topic_id
                    );

                    system
                        .write()
                        .await
                        .metrics
                        .decrement_segments(deleted_segments.segments_count);
                    system
                        .write()
                        .await
                        .metrics
                        .decrement_messages(deleted_segments.messages_count);
                }
            }
        }
    }
}

async fn delete_expired_segments(
    topic: &Topic,
    now: u64,
) -> Result<Option<DeletedSegments>, Error> {
    let expired_segments = topic
        .get_expired_segments_start_offsets_per_partition(now)
        .await;
    if expired_segments.is_empty() {
        info!(
            "No expired segments found for stream ID: {}, topic ID: {}",
            topic.stream_id, topic.topic_id
        );
        return Ok(None);
    }

    info!(
        "Found {} expired segments for stream ID: {}, topic ID: {}, deleting...",
        expired_segments.len(),
        topic.stream_id,
        topic.topic_id
    );

    let mut segments_count = 0;
    let mut messages_count = 0;
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
            segments_count += 1;
            messages_count += deleted_segment.get_messages_count();
        }

        if partition.get_segments().is_empty() {
            let start_offset = last_end_offset + 1;
            partition.add_persisted_segment(start_offset).await?;
        }
    }

    Ok(Some(DeletedSegments {
        segments_count,
        messages_count,
    }))
}

struct DeletedSegments {
    pub segments_count: u32,
    pub messages_count: u64,
}
