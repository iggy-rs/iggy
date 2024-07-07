use crate::archiver::Archiver;
use crate::streaming::systems::system::SharedSystem;
use crate::streaming::topics::topic::Topic;
use crate::{channels::server_command::ServerCommand, configs::server::MessageCleanerConfig};
use async_trait::async_trait;
use flume::Sender;
use iggy::error::IggyError;
use iggy::locking::IggySharedMutFn;
use iggy::utils::duration::IggyDuration;
use iggy::utils::timestamp::IggyTimestamp;
use std::sync::Arc;
use tokio::time;
use tracing::{debug, error, info};

pub struct MessagesCleaner {
    enabled: bool,
    interval: IggyDuration,
    sender: Sender<CleanMessagesCommand>,
}

#[derive(Debug, Default, Clone)]
pub struct CleanMessagesCommand;

#[derive(Debug, Default, Clone)]
pub struct CleanMessagesExecutor;

impl MessagesCleaner {
    pub fn new(config: &MessageCleanerConfig, sender: Sender<CleanMessagesCommand>) -> Self {
        Self {
            enabled: config.enabled,
            interval: config.interval,
            sender,
        }
    }

    pub fn start(&self) {
        if !self.enabled {
            info!("Message cleaner is disabled.");
            return;
        }

        let interval = self.interval;
        let sender = self.sender.clone();
        info!(
            "Message cleaner is enabled, expired messages will be deleted every: {:?}.",
            interval
        );

        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval.get_duration());
            loop {
                interval_timer.tick().await;
                sender.send(CleanMessagesCommand).unwrap_or_else(|err| {
                    error!("Failed to send CleanMessagesCommand. Error: {}", err);
                });
            }
        });
    }
}

#[async_trait]
impl ServerCommand<CleanMessagesCommand> for CleanMessagesExecutor {
    async fn execute(&mut self, system: &SharedSystem, _command: CleanMessagesCommand) {
        let system = system.read();
        let streams = system.get_streams();
        for stream in streams {
            let topics = stream.get_topics();
            for topic in topics {
                let deleted_expired_segments = handle_expired_segments(
                    topic,
                    system.archiver.clone(),
                    system.config.segment.archive_expired,
                )
                .await;
                if deleted_expired_segments.is_err() {
                    error!(
                        "Failed to delete expired segments for stream ID: {}, topic ID: {}",
                        topic.stream_id, topic.topic_id
                    );
                    continue;
                }

                let deleted_oldest_segments = handle_oldest_segments(
                    topic,
                    system.archiver.clone(),
                    system.config.topic.delete_oldest_segments,
                )
                .await;
                if deleted_oldest_segments.is_err() {
                    error!(
                        "Failed to delete oldest segments for stream ID: {}, topic ID: {}",
                        topic.stream_id, topic.topic_id
                    );
                    continue;
                }

                let deleted_expired_segments = deleted_expired_segments.unwrap();
                let deleted_oldest_segments = deleted_oldest_segments.unwrap();
                let deleted_segments = HandledSegments {
                    segments_count: deleted_expired_segments.segments_count
                        + deleted_oldest_segments.segments_count,
                    messages_count: deleted_expired_segments.messages_count
                        + deleted_oldest_segments.messages_count,
                };

                if deleted_segments.segments_count == 0 {
                    debug!(
                        "No segments were deleted for stream ID: {}, topic ID: {}",
                        topic.stream_id, topic.topic_id
                    );
                    continue;
                }

                info!(
                    "Deleted {} segments and {} messages for stream ID: {}, topic ID: {}",
                    deleted_segments.segments_count,
                    deleted_segments.messages_count,
                    topic.stream_id,
                    topic.topic_id
                );

                system
                    .metrics
                    .decrement_segments(deleted_segments.segments_count);
                system
                    .metrics
                    .decrement_messages(deleted_segments.messages_count);
            }
        }
    }

    fn start_command_sender(
        &mut self,
        _system: SharedSystem,
        config: &crate::configs::server::ServerConfig,
        sender: Sender<CleanMessagesCommand>,
    ) {
        let messages_cleaner = MessagesCleaner::new(&config.message_cleaner, sender);
        messages_cleaner.start();
    }

    fn start_command_consumer(
        mut self,
        system: SharedSystem,
        _config: &crate::configs::server::ServerConfig,
        receiver: flume::Receiver<CleanMessagesCommand>,
    ) {
        tokio::spawn(async move {
            let system = system.clone();
            while let Ok(command) = receiver.recv_async().await {
                self.execute(&system, command).await;
            }
            info!("Messages cleaner receiver stopped.");
        });
    }
}

async fn handle_expired_segments(
    topic: &Topic,
    archiver: Option<Arc<dyn Archiver>>,
    archive_expired: bool,
) -> Result<HandledSegments, IggyError> {
    let expired_segments = get_expired_segments(topic, IggyTimestamp::now()).await;
    if expired_segments.is_empty() {
        return Ok(HandledSegments::none());
    }

    if archive_expired {
        if let Some(archiver) = archiver {
            archive_segments(topic, &expired_segments, archiver.clone()).await?;
        } else {
            error!(
                "Archiver is not enabled, yet archive_expired is set to true. Cannot archive expired segments for stream ID: {}, topic ID: {}",
                topic.stream_id, topic.topic_id
            );
            return Ok(HandledSegments::none());
        }
    }

    delete_segments(topic, &expired_segments).await
}

async fn get_expired_segments(topic: &Topic, now: IggyTimestamp) -> Vec<SegmentsToHandle> {
    let expired_segments = topic
        .get_expired_segments_start_offsets_per_partition(now)
        .await;
    if expired_segments.is_empty() {
        debug!(
            "No expired segments found for stream ID: {}, topic ID: {}",
            topic.stream_id, topic.topic_id
        );
        return Vec::new();
    }

    info!(
        "Found {} expired segments to delete for stream ID: {}, topic ID: {}",
        expired_segments.len(),
        topic.stream_id,
        topic.topic_id
    );

    expired_segments
        .into_iter()
        .map(|(partition_id, start_offsets)| SegmentsToHandle {
            partition_id,
            start_offsets,
        })
        .collect()
}

async fn handle_oldest_segments(
    topic: &Topic,
    archiver: Option<Arc<dyn Archiver>>,
    delete_oldest_segments: bool,
) -> Result<HandledSegments, IggyError> {
    if let Some(archiver) = archiver {
        let mut segments_to_archive = Vec::new();
        for partition in topic.partitions.values() {
            let mut start_offsets = Vec::new();
            let partition = partition.read().await;
            for segment in partition.get_segments() {
                let is_archived = archiver
                    .is_archived(
                        topic.stream_id,
                        topic.topic_id,
                        partition.partition_id,
                        segment.start_offset,
                    )
                    .await;
                if is_archived.is_err() {
                    error!(
                        "Failed to check if segment with start offset: {} is archived for stream ID: {}, topic ID: {}. Error: {}",
                        segment.start_offset, topic.stream_id, topic.topic_id, is_archived.err().unwrap()
                    );
                    continue;
                }

                if !is_archived.unwrap() {
                    debug!(
                        "Segment with start offset: {} is not archived for stream ID: {}, topic ID: {}",
                        segment.start_offset, topic.stream_id, topic.topic_id
                    );
                    start_offsets.push(segment.start_offset);
                }
            }
            if !start_offsets.is_empty() {
                debug!(
                    "Found {} segments to archive for stream ID: {}, topic ID: {}",
                    start_offsets.len(),
                    topic.stream_id,
                    topic.topic_id
                );
                segments_to_archive.push(SegmentsToHandle {
                    partition_id: partition.partition_id,
                    start_offsets,
                });
            }
        }

        archive_segments(topic, &segments_to_archive, archiver.clone()).await?;
    }

    if topic.is_unlimited() {
        debug!(
            "Topic is unlimited, oldest segments will not be deleted for stream ID: {}, topic ID: {}",
            topic.stream_id, topic.topic_id
        );
        return Ok(HandledSegments::none());
    }

    if !delete_oldest_segments {
        debug!(
            "Delete oldest segments is disabled, oldest segments will not be deleted for stream ID: {}, topic ID: {}",
            topic.stream_id, topic.topic_id
        );
        return Ok(HandledSegments::none());
    }

    if !topic.is_almost_full() {
        debug!(
            "Topic is not almost full, oldest segments will not be deleted for stream ID: {}, topic ID: {}",
            topic.stream_id, topic.topic_id
        );
        return Ok(HandledSegments::none());
    }

    let oldest_segments = get_oldest_segments(topic).await;
    if oldest_segments.is_empty() {
        return Ok(HandledSegments::none());
    }

    delete_segments(topic, &oldest_segments).await
}

async fn get_oldest_segments(topic: &Topic) -> Vec<SegmentsToHandle> {
    let mut segments_to_delete = Vec::new();
    for partition in topic.partitions.values() {
        let partition = partition.read().await;
        if let Some(segment) = partition.get_segments().first() {
            if !segment.is_closed {
                continue;
            }

            segments_to_delete.push(SegmentsToHandle {
                partition_id: partition.partition_id,
                start_offsets: vec![segment.start_offset],
            });
        }
    }

    if segments_to_delete.is_empty() {
        return segments_to_delete;
    }

    info!(
        "Found {} start segments to delete for stream ID: {}, topic ID: {}.",
        segments_to_delete.len(),
        topic.stream_id,
        topic.topic_id
    );

    segments_to_delete
}

#[derive(Debug)]
struct SegmentsToHandle {
    partition_id: u32,
    start_offsets: Vec<u64>,
}

#[derive(Debug)]
struct HandledSegments {
    pub segments_count: u32,
    pub messages_count: u64,
}

impl HandledSegments {
    pub fn none() -> Self {
        Self {
            segments_count: 0,
            messages_count: 0,
        }
    }
}

async fn archive_segments(
    topic: &Topic,
    segments_to_archive: &[SegmentsToHandle],
    archiver: Arc<dyn Archiver>,
) -> Result<u64, IggyError> {
    if segments_to_archive.is_empty() {
        return Ok(0);
    }

    info!(
        "Found {} segments to archive for stream ID: {}, topic ID: {}, archiving...",
        segments_to_archive.len(),
        topic.stream_id,
        topic.topic_id
    );

    let mut archived_segments = 0;
    for segment_to_archive in segments_to_archive {
        match topic.get_partition(segment_to_archive.partition_id) {
            Ok(partition) => {
                let partition = partition.read().await;
                for start_offset in &segment_to_archive.start_offsets {
                    let segment = partition.get_segment(*start_offset);
                    if segment.is_none() {
                        error!(
                            "Segment with start offset: {} not found for stream ID: {}, topic ID: {}",
                            start_offset, topic.stream_id, topic.topic_id
                        );
                        continue;
                    }

                    let archivable_segment = segment.unwrap().into();
                    if let Err(error) = archiver.archive(archivable_segment).await {
                        error!(
                            "Failed to archive segment with start offset: {} for stream ID: {}, topic ID: {}. Error: {}",
                            start_offset, topic.stream_id, topic.topic_id, error
                        );
                        continue;
                    }
                    archived_segments += 1;
                }
            }
            Err(error) => {
                error!(
                    "Partition with ID: {} not found for stream ID: {}, topic ID: {}. Error: {}",
                    segment_to_archive.partition_id, topic.stream_id, topic.topic_id, error
                );
                continue;
            }
        }
    }

    Ok(archived_segments)
}

async fn delete_segments(
    topic: &Topic,
    segments_to_delete: &[SegmentsToHandle],
) -> Result<HandledSegments, IggyError> {
    info!(
        "Found {} segments to delete for stream ID: {}, topic ID: {}, deleting...",
        segments_to_delete.len(),
        topic.stream_id,
        topic.topic_id
    );

    let mut segments_count = 0;
    let mut messages_count = 0;
    for segment_to_delete in segments_to_delete {
        match topic.get_partition(segment_to_delete.partition_id) {
            Ok(partition) => {
                let mut partition = partition.write().await;
                let mut last_end_offset = 0;
                for start_offset in &segment_to_delete.start_offsets {
                    let deleted_segment = partition.delete_segment(*start_offset).await?;
                    last_end_offset = deleted_segment.end_offset;
                    segments_count += 1;
                    messages_count += deleted_segment.messages_count;
                }

                if partition.get_segments().is_empty() {
                    let start_offset = last_end_offset + 1;
                    partition.add_persisted_segment(start_offset).await?;
                }
            }
            Err(error) => {
                error!(
                    "Partition with ID: {} not found for stream ID: {}, topic ID: {}. Error: {}",
                    segment_to_delete.partition_id, topic.stream_id, topic.topic_id, error
                );
                continue;
            }
        }
    }

    Ok(HandledSegments {
        segments_count,
        messages_count,
    })
}
