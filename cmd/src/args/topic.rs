use crate::args::common::ListMode;
use clap::{Args, Subcommand};
use iggy::cmd::utils::message_expiry::MessageExpiry;
use iggy::identifier::Identifier;
use std::convert::From;

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum TopicAction {
    /// Create topic with given ID, name, number of partitions
    /// and expiry time for given stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    ///
    /// Examples
    ///  iggy topic create 1 1 2 sensor1 15days
    ///  iggy topic create prod 2 2 sensor2
    ///  iggy topic create test 3 2 debugs 1day 1hour 1min 1sec
    #[clap(verbatim_doc_comment)]
    Create(TopicCreateArgs),
    /// Delete topic with given ID in given stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    ///
    /// Examples
    ///  iggy topic delete 1 1
    ///  iggy topic delete prod 2
    ///  iggy topic delete test debugs
    ///  iggy topic delete 2 debugs
    #[clap(verbatim_doc_comment)]
    Delete(TopicDeleteArgs),
    /// Update topic name an message expiry time for given topic ID in given stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    ///
    /// Examples
    ///  iggy update 1 1 sensor3
    ///  iggy update prod sensor3 old-sensor
    ///  iggy update test debugs ready 15days
    ///  iggy update 1 1 new-name
    ///  iggy update 1 2 new-name 1day 1hour 1min 1sec
    #[clap(verbatim_doc_comment)]
    Update(TopicUpdateArgs),
    /// Get topic detail for given topic ID and stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    ///
    /// Examples
    ///  iggy topic get 1 1
    ///  iggy topic get prod 2
    ///  iggy topic get test debugs
    ///  iggy topic get 2 debugs
    #[clap(verbatim_doc_comment)]
    Get(TopicGetArgs),
    /// List all topics in given stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    ///
    /// Examples
    ///  iggy topic list 1
    ///  iggy topic list prod
    #[clap(verbatim_doc_comment)]
    List(TopicListArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct TopicCreateArgs {
    /// Stream ID to create topic
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Topic ID to create
    pub(crate) topic_id: u32,
    /// Number of partitions inside the topic
    pub(crate) partitions_count: u32,
    /// Name of the topic
    pub(crate) name: String,
    /// Message expiry time in human readable format like 15days 2min 2s
    /// ("none" or skipping parameter disables message expiry functionality in topic)
    #[arg(value_parser = clap::value_parser!(MessageExpiry))]
    pub(crate) message_expiry: Option<Vec<MessageExpiry>>,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct TopicDeleteArgs {
    /// Stream ID to delete topic
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Topic ID to delete
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct TopicUpdateArgs {
    /// Stream ID to update topic
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Topic ID to update
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
    /// New name for the topic
    pub(crate) name: String,
    /// New message expiry time in human readable format like 15days 2min 2s
    /// ("none" or skipping parameter causes removal of expiry parameter in topic)
    #[arg(value_parser = clap::value_parser!(MessageExpiry))]
    pub(crate) message_expiry: Option<Vec<MessageExpiry>>,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct TopicGetArgs {
    /// Stream ID to get topic
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Topic ID to get
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct TopicListArgs {
    /// Stream ID to list topics
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,

    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}
