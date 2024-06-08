use crate::sourcing::metadata::MetadataEntry;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::*;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::partitions::create_partitions::CreatePartitions;
use iggy::partitions::delete_partitions::DeletePartitions;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::streams::update_stream::UpdateStream;
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::delete_topic::DeleteTopic;
use iggy::topics::update_topic::UpdateTopic;
use std::collections::HashMap;
use std::fmt::Display;
use tracing::{info, warn};

#[derive(Debug)]
pub struct SystemView {
    pub streams: HashMap<Identifier, StreamView>,
}

#[derive(Debug)]
pub struct StreamView {
    pub id: u32,
    pub name: String,
    pub topics: HashMap<Identifier, TopicView>,
    current_topic_id: u32,
}

#[derive(Debug)]
pub struct TopicView {
    pub id: u32,
    pub name: String,
    pub partitions: HashMap<Identifier, PartitionView>,
}

#[derive(Debug)]
pub struct PartitionView {
    pub id: u32,
}

impl SystemView {
    pub async fn init(entries: Vec<MetadataEntry>) -> Result<Self, IggyError> {
        let mut streams = HashMap::new();
        let mut current_stream_id = 0;
        for entry in entries {
            info!(
                "Processing metadata entry code: {}, name: {}",
                entry.code,
                get_name_from_code(entry.code).unwrap_or("invalid_command")
            );
            match entry.code {
                CREATE_STREAM_CODE => {
                    let command = CreateStream::from_bytes(entry.data)?;
                    let stream_id = command.stream_id.unwrap_or_else(|| {
                        current_stream_id += 1;
                        current_stream_id
                    });
                    let stream = StreamView {
                        id: stream_id,
                        name: command.name,
                        topics: HashMap::new(),
                        current_topic_id: 0,
                    };
                    streams.insert(stream.id.try_into()?, stream);
                }
                UPDATE_STREAM_CODE => {
                    let command = UpdateStream::from_bytes(entry.data)?;
                    let stream = streams.get_mut(&command.stream_id).unwrap();
                    stream.name = command.name;
                }
                DELETE_STREAM_CODE => {
                    let command = DeleteStream::from_bytes(entry.data)?;
                    streams.remove(&command.stream_id);
                }
                CREATE_TOPIC_CODE => {
                    let command = CreateTopic::from_bytes(entry.data)?;
                    let stream = streams.get_mut(&command.stream_id).unwrap();
                    let topic_id = command.topic_id.unwrap_or_else(|| {
                        stream.current_topic_id += 1;
                        stream.current_topic_id
                    });
                    let topic = TopicView {
                        id: topic_id,
                        name: command.name,
                        partitions: if command.partitions_count == 0 {
                            HashMap::new()
                        } else {
                            (1..=command.partitions_count)
                                .map(|id| (id.try_into().unwrap(), PartitionView { id }))
                                .collect()
                        },
                    };
                    stream.topics.insert(topic.id.try_into()?, topic);
                }
                UPDATE_TOPIC_CODE => {
                    let command = UpdateTopic::from_bytes(entry.data)?;
                    let stream = streams.get_mut(&command.stream_id).unwrap();
                    let topic = stream.topics.get_mut(&command.topic_id).unwrap();
                    topic.name = command.name;
                }
                DELETE_TOPIC_CODE => {
                    let command = DeleteTopic::from_bytes(entry.data)?;
                    let stream = streams.get_mut(&command.stream_id).unwrap();
                    stream.topics.remove(&command.topic_id);
                }
                CREATE_PARTITIONS_CODE => {
                    let command = CreatePartitions::from_bytes(entry.data)?;
                    let stream = streams.get_mut(&command.stream_id).unwrap();
                    let topic = stream.topics.get_mut(&command.topic_id).unwrap();
                    let last_partition_id =
                        topic.partitions.iter().map(|(_, p)| p.id).max().unwrap();
                    for id in last_partition_id + 1..=last_partition_id + command.partitions_count {
                        topic
                            .partitions
                            .insert(id.try_into()?, PartitionView { id });
                    }
                }
                DELETE_PARTITIONS_CODE => {
                    let command = DeletePartitions::from_bytes(entry.data)?;
                    let stream = streams.get_mut(&command.stream_id).unwrap();
                    let topic = stream.topics.get_mut(&command.topic_id).unwrap();
                    let last_partition_id =
                        topic.partitions.iter().map(|(_, p)| p.id).max().unwrap();
                    for id in last_partition_id - command.partitions_count + 1..=last_partition_id {
                        topic.partitions.remove(&id.try_into().unwrap());
                    }
                }
                code => {
                    warn!("Unsupported metadata entry code: {code}");
                }
            }
        }
        Ok(SystemView { streams })
    }
}

impl Display for SystemView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Streams:")?;
        for stream in self.streams.iter() {
            write!(f, "\n================\n")?;
            write!(f, "{}\n", stream.1)?;
        }
        Ok(())
    }
}

impl Display for StreamView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stream -> ID: {}, Name: {}", self.id, self.name,)?;
        for topic in self.topics.iter() {
            write!(f, "\n {}", topic.1)?;
        }
        Ok(())
    }
}

impl Display for TopicView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Topic -> ID: {}, Name: {}", self.id, self.name,)?;
        for partition in self.partitions.iter() {
            write!(f, "\n  {}", partition.1)?;
        }
        Ok(())
    }
}

impl Display for PartitionView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Partition -> ID: {}", self.id)
    }
}
