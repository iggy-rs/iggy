use crate::args::common::ListMode;
use clap::{Args, Subcommand};
use humantime::format_duration;
use humantime::Duration as HumanDuration;
use iggy::identifier::Identifier;
use std::fmt::Display;
use std::iter::Sum;
use std::ops::Add;
use std::time::Duration;
use std::{convert::From, str::FromStr};

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

#[derive(Debug, Args)]
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

#[derive(Debug, Args)]
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum MessageExpiry {
    /// Set message expiry time to given value
    ExpireDuration(Duration),
    /// Never expire messages
    NeverExpire,
}

impl MessageExpiry {
    pub(crate) fn new(values: Option<Vec<MessageExpiry>>) -> Option<Self> {
        values.map(|items| items.iter().cloned().sum())
    }
}

impl From<&MessageExpiry> for Option<u32> {
    fn from(value: &MessageExpiry) -> Self {
        match value {
            MessageExpiry::ExpireDuration(value) => Some(value.as_secs() as u32),
            MessageExpiry::NeverExpire => None,
        }
    }
}

impl Display for MessageExpiry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NeverExpire => write!(f, "none"),
            Self::ExpireDuration(value) => write!(f, "{}", format_duration(*value)),
        }
    }
}

impl Sum for MessageExpiry {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.into_iter()
            .fold(MessageExpiry::NeverExpire, |acc, x| acc + x)
    }
}

impl Add for MessageExpiry {
    type Output = MessageExpiry;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (MessageExpiry::NeverExpire, MessageExpiry::NeverExpire) => MessageExpiry::NeverExpire,
            (MessageExpiry::NeverExpire, message_expiry) => message_expiry,
            (message_expiry, MessageExpiry::NeverExpire) => message_expiry,
            (
                MessageExpiry::ExpireDuration(lhs_duration),
                MessageExpiry::ExpireDuration(rhs_duration),
            ) => MessageExpiry::ExpireDuration(lhs_duration + rhs_duration),
        }
    }
}

impl FromStr for MessageExpiry {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "none" => MessageExpiry::NeverExpire,
            value => {
                let duration = value.parse::<HumanDuration>().map_err(|e| format!("{e}"))?;

                if duration.as_secs() > u32::MAX as u64 {
                    return Err(format!(
                        "Value too big for message expiry time, maximum value is {}",
                        format_duration(Duration::from_secs(u32::MAX as u64))
                    ));
                }

                MessageExpiry::ExpireDuration(Duration::from_secs(duration.as_secs()))
            }
        };

        Ok(result)
    }
}

#[derive(Debug, Args)]
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

#[derive(Debug, Args)]
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
