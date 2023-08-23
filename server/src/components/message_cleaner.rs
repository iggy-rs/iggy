use iggy::utils::timestamp;
use std::sync::Arc;
use std::time::Duration;
use streaming::systems::system::System;
use streaming::topics::topic::Topic;
use tokio::sync::RwLock;
use tokio::{task, time};
use tracing::info;

pub fn start(system: Arc<RwLock<System>>) {
    let duration = Duration::from_secs(10);
    task::spawn(async move {
        let mut interval = time::interval(duration);
        interval.tick().await;
        loop {
            interval.tick().await;
            info!("Message cleaner policy is running...");
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

async fn delete_expired_segments(topic: &Topic, now: u64) {
    let expired_segments = topic
        .get_expired_segments_start_offsets_per_partition(now)
        .await;
    if expired_segments.is_empty() {
        return;
    }

    // TODO: Delete expired segments
    info!(
        "Found {} expired segments for stream ID: {}, topic ID: {}, deleting...",
        expired_segments.len(),
        topic.stream_id,
        topic.id
    );
}
