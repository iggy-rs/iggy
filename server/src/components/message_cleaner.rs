use iggy::utils::timestamp;
use std::sync::Arc;
use std::time::Duration;
use streaming::systems::system::System;
use streaming::topics::topic::Topic;
use tokio::sync::RwLock;
use tokio::{task, time};
use tracing::{error, info};

pub fn start(system: Arc<RwLock<System>>) {
    let duration = Duration::from_secs(60);
    task::spawn(async move {
        let mut interval = time::interval(duration);
        interval.tick().await;
        loop {
            interval.tick().await;
            let system = system.read().await;
            let now = timestamp::get();
            let streams = system.get_streams();
            for stream in streams {
                let topics = stream.get_topics();
                for topic in topics {
                    delete_expired_segments(topic, now).await;
                }
            }
        }
    });
}

// TODO: When the only segment is removed from partition, it should somehow store its current offset
async fn delete_expired_segments(topic: &Topic, now: u64) {
    let expired_segments = topic
        .get_expired_segments_start_offsets_per_partition(now)
        .await;
    if expired_segments.is_empty() {
        info!(
            "No expired segments found for stream ID: {}, topic ID: {}",
            topic.stream_id, topic.id
        );
        return;
    }

    info!(
        "Found {} expired segments for stream ID: {}, topic ID: {}, deleting...",
        expired_segments.len(),
        topic.stream_id,
        topic.id
    );

    for (partition_id, start_offsets) in &expired_segments {
        let partition = topic.get_partition(*partition_id);
        if partition.is_err() {
            error!(
                "Partition with ID: {} not found for stream ID: {}, topic ID: {}",
                partition_id, topic.stream_id, topic.id
            );
            continue;
        }
        let partition = partition.unwrap();
        let mut partition = partition.write().await;
        for start_offset in start_offsets {
            if partition.delete_segment(*start_offset).await.is_err() {
                error!(
                    "Failed to delete segment with start offset: {} for partition with ID: {} and topic with ID: {} and stream with ID: {}",
                    start_offset, partition.id, partition.topic_id, partition.stream_id
                );
            }
        }
    }
}
