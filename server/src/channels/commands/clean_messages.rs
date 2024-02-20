use crate::streaming::systems::system::SharedSystem;
use crate::streaming::topics::topic::Topic;
use crate::{channels::server_command::ServerCommand, configs::server::MessageCleanerConfig};
use async_trait::async_trait;
use flume::Sender;
use iggy::error::IggyError;
use iggy::locking::IggySharedMutFn;
use iggy::utils::duration::IggyDuration;
use iggy::utils::timestamp::IggyTimestamp;
use tokio::time;
use tracing::{error, info};

struct DeletedSegments {
    pub segments_count: u32,
    pub messages_count: u64,
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
        let now = IggyTimestamp::now().to_micros();
        let system_read = system.read();
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
                        .metrics
                        .decrement_segments(deleted_segments.segments_count);
                    system
                        .write()
                        .metrics
                        .decrement_messages(deleted_segments.messages_count);
                }
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

async fn delete_expired_segments(
    topic: &Topic,
    now: u64,
) -> Result<Option<DeletedSegments>, IggyError> {
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
        match topic.get_partition(*partition_id) {
            Ok(partition) => {
                let mut partition = partition.write().await;
                let mut last_end_offset = 0;
                for start_offset in start_offsets {
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
                    partition_id, topic.stream_id, topic.topic_id, error
                );
                continue;
            }
        }
    }

    Ok(Some(DeletedSegments {
        segments_count,
        messages_count,
    }))
}
