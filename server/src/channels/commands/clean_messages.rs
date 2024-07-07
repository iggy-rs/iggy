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

struct DeletedSegments {
    pub segments_count: u32,
    pub messages_count: u64,
}

impl DeletedSegments {
    pub fn none() -> Self {
        Self {
            segments_count: 0,
            messages_count: 0,
        }
    }
}

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
        let now = IggyTimestamp::now();
        let system = system.read();
        let streams = system.get_streams();
        for stream in streams {
            let topics = stream.get_topics();
            for topic in topics {
                let deleted_expired_segments =
                    clean_expired_segments(topic, now, system.archiver.clone()).await;
                if deleted_expired_segments.is_err() {
                    error!(
                        "Failed to delete expired segments for stream ID: {}, topic ID: {}",
                        topic.stream_id, topic.topic_id
                    );
                    continue;
                }

                let deleted_start_segments =
                    clean_start_segments(topic, system.archiver.clone()).await;
                if deleted_start_segments.is_err() {
                    error!(
                        "Failed to delete start segments for stream ID: {}, topic ID: {}",
                        topic.stream_id, topic.topic_id
                    );
                    continue;
                }

                let deleted_expired_segments = deleted_expired_segments.unwrap();
                let deleted_start_segments = deleted_start_segments.unwrap();
                let deleted_segments = DeletedSegments {
                    segments_count: deleted_expired_segments.segments_count
                        + deleted_start_segments.segments_count,
                    messages_count: deleted_expired_segments.messages_count
                        + deleted_start_segments.messages_count,
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

async fn clean_expired_segments(
    topic: &Topic,
    now: IggyTimestamp,
    archiver: Option<Arc<dyn Archiver>>,
) -> Result<DeletedSegments, IggyError> {
    let expired_segments = topic
        .get_expired_segments_start_offsets_per_partition(now)
        .await;
    if expired_segments.is_empty() {
        debug!(
            "No expired segments found for stream ID: {}, topic ID: {}",
            topic.stream_id, topic.topic_id
        );
        return Ok(DeletedSegments::none());
    }

    info!(
        "Found {} expired segments to delete for stream ID: {}, topic ID: {}",
        expired_segments.len(),
        topic.stream_id,
        topic.topic_id
    );

    let segments_to_delete = expired_segments
        .into_iter()
        .map(|(partition_id, start_offsets)| SegmentsToDelete {
            partition_id,
            start_offsets,
        })
        .collect();
    archive_and_delete_segments(topic, segments_to_delete, archiver).await
}

async fn clean_start_segments(
    topic: &Topic,
    archiver: Option<Arc<dyn Archiver>>,
) -> Result<DeletedSegments, IggyError> {
    if !topic.is_full() {
        return Ok(DeletedSegments::none());
    }

    let mut segments_to_delete = Vec::new();
    for partition in topic.partitions.values() {
        let partition = partition.read().await;
        if let Some(segment) = partition.get_segments().first() {
            if !segment.is_closed {
                continue;
            }

            segments_to_delete.push(SegmentsToDelete {
                partition_id: partition.partition_id,
                start_offsets: vec![segment.start_offset],
            });
        }
    }

    if segments_to_delete.is_empty() {
        return Ok(DeletedSegments::none());
    }

    info!(
        "Found {} start segments to delete for stream ID: {}, topic ID: {}.",
        segments_to_delete.len(),
        topic.stream_id,
        topic.topic_id
    );

    archive_and_delete_segments(topic, segments_to_delete, archiver).await
}

#[derive(Debug)]
struct SegmentsToDelete {
    partition_id: u32,
    start_offsets: Vec<u64>,
}

async fn archive_and_delete_segments(
    topic: &Topic,
    segments_to_delete: Vec<SegmentsToDelete>,
    archiver: Option<Arc<dyn Archiver>>,
) -> Result<DeletedSegments, IggyError> {
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
                for start_offset in segment_to_delete.start_offsets {
                    if let Some(archiver) = archiver.as_ref() {
                        let segment = partition.get_segment(start_offset);
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
                    }

                    let deleted_segment = partition.delete_segment(start_offset).await?;
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

    Ok(DeletedSegments {
        segments_count,
        messages_count,
    })
}
