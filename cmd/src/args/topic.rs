use crate::args::common::ListMode;
use clap::{Args, Subcommand};

#[derive(Debug, Subcommand)]
pub(crate) enum TopicAction {
    /// Create topic with given ID, name, number of partitions
    /// and expiry time for given stream ID
    Create(TopicCreateArgs),
    /// Delete topic with given ID in given stream ID
    Delete(TopicDeleteArgs),
    /// Update topic name an message expiry time for given topic ID in given stream ID
    Update(TopicUpdateArgs),
    /// Get topic detail for given topic ID and stream ID
    Get(TopicGetArgs),
    /// List all topics in given stream ID
    List(TopicListArgs),
}

#[derive(Debug, Args)]
pub(crate) struct TopicCreateArgs {
    /// Stream ID to create topic
    pub(crate) stream_id: u32,
    /// Topic ID to create
    pub(crate) topic_id: u32,
    /// Number of partitions inside the topic
    pub(crate) partitions_count: u32,
    /// Name of the topic
    pub(crate) name: String,
    /// Message expiry time in seconds
    pub(crate) message_expiry: Option<u32>,
}

#[derive(Debug, Args)]
pub(crate) struct TopicDeleteArgs {
    /// Stream ID to delete topic
    pub(crate) stream_id: u32,
    /// Topic ID to delete
    pub(crate) topic_id: u32,
}

#[derive(Debug, Args)]
pub(crate) struct TopicUpdateArgs {
    /// Stream ID to update topic
    pub(crate) stream_id: u32,
    /// Topic ID to update
    pub(crate) topic_id: u32,
    /// New name for the topic
    pub(crate) name: String,
    /// New message expiry time in seconds
    /// (skipping parameter causes removal of expiry parameter in topic)
    pub(crate) message_expiry: Option<u32>,
}

#[derive(Debug, Args)]
pub(crate) struct TopicGetArgs {
    /// Stream ID to get topic
    pub(crate) stream_id: u32,
    /// Topic ID to get
    pub(crate) topic_id: u32,
}

#[derive(Debug, Args)]
pub(crate) struct TopicListArgs {
    /// Stream ID to list topics
    pub(crate) stream_id: u32,

    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}
