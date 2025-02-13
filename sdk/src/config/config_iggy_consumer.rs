use bon::Builder;

use crate::clients::consumer::{AutoCommit, AutoCommitWhen};
use crate::config::shared_config;
use crate::consumer::ConsumerKind;
use crate::identifier::Identifier;
use crate::messages::poll_messages::PollingStrategy;
use crate::utils::duration::IggyDuration;
use std::str::FromStr;

#[derive(Builder, Debug, Clone)]
#[builder(on(String, into))]
pub struct IggyConsumerConfig {
    stream_id: Identifier,
    stream_name: String,
    topic_id: Identifier,
    topic_name: String,
    auto_commit: AutoCommit,
    batch_size: u32,
    consumer_name: String,
    consumer_kind: ConsumerKind,
    polling_interval: IggyDuration,
    polling_strategy: PollingStrategy,
    partitions_count: u32,
    replication_factor: Option<u8>,
}

impl Default for IggyConsumerConfig {
    fn default() -> Self {
        let stream_id = shared_config::get_identifier_from_string("test_stream");
        let topic_id = shared_config::get_identifier_from_string("test_topic");

        Self {
            stream_id,
            stream_name: "test_stream".to_string(),
            topic_id,
            topic_name: "test_topic".to_string(),
            auto_commit: AutoCommit::When(AutoCommitWhen::PollingMessages),
            batch_size: 100,
            consumer_name: "test_consumer".to_string(),
            consumer_kind: ConsumerKind::ConsumerGroup,
            polling_interval: IggyDuration::from_str("5ms").unwrap(),
            polling_strategy: PollingStrategy::last(),
            partitions_count: 1,
            replication_factor: None,
        }
    }
}

impl IggyConsumerConfig {
    /// Creates a new `IggyConsumerConfig` from the given arguments.
    ///
    /// # Args
    ///
    /// * `stream_id` - The stream identifier.
    /// * `stream_name` - The stream name.
    /// * `topic_id` - The topic identifier.
    /// * `topic_name` - The topic name.
    /// * `auto_commit` - The auto-commit configuration to use.
    /// * `batch_size` - The max number of messages to send in a batch.
    /// * `consumer_name` - The name of the consumer group.
    /// * `consumer_kind` - The consumer kind to use.
    /// * `polling_interval` - The interval between polling for new messages.
    /// * `polling_strategy` - The polling strategy to use.
    /// * `partition` - The number of partitions to create.
    ///
    /// Returns:
    /// A new `IggyConsumerConfig`.
    ///
    pub fn new(
        stream_id: Identifier,
        stream_name: String,
        topic_id: Identifier,
        topic_name: String,
        auto_commit: AutoCommit,
        batch_size: u32,
        consumer_name: String,
        consumer_kind: ConsumerKind,
        polling_interval: IggyDuration,
        polling_strategy: PollingStrategy,
        partitions_count: u32,
        replication_factor: Option<u8>,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            auto_commit,
            batch_size,
            consumer_name,
            consumer_kind,
            polling_interval,
            polling_strategy,
            partitions_count,
            replication_factor,
        }
    }

    /// Creates a new `IggyConsumerConfig` from the given arguments.
    ///
    /// # Args
    ///
    /// * `stream` - The stream name.
    /// * `topic` - The topic name.
    /// * `batch_size` - The max number of messages to send in a batch.
    /// * `polling_interval` - The interval between polling for new messages.
    ///
    /// Returns:
    /// A new `IggyConsumerConfig`.
    ///
    pub fn from_stream_topic(
        stream: &str,
        topic: &str,
        batch_size: u32,
        polling_interval: IggyDuration,
    ) -> Self {
        let stream_id = shared_config::get_identifier_from_string(stream);
        let topic_id = shared_config::get_identifier_from_string(topic);

        Self {
            stream_id,
            stream_name: stream.to_string(),
            topic_id,
            topic_name: topic.to_string(),
            auto_commit: AutoCommit::When(AutoCommitWhen::PollingMessages),
            batch_size,
            consumer_name: format!("consumer-{}-{}", stream, topic),
            consumer_kind: ConsumerKind::ConsumerGroup,
            polling_interval,
            polling_strategy: PollingStrategy::next(),
            partitions_count: 1,
            replication_factor: None,
        }
    }
}

impl IggyConsumerConfig {
    pub fn stream_id(&self) -> &Identifier {
        &self.stream_id
    }

    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    pub fn topic_id(&self) -> &Identifier {
        &self.topic_id
    }

    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    pub fn auto_commit(&self) -> AutoCommit {
        self.auto_commit
    }

    pub fn batch_size(&self) -> u32 {
        self.batch_size
    }

    pub fn consumer_name(&self) -> &str {
        &self.consumer_name
    }

    pub fn consumer_kind(&self) -> ConsumerKind {
        self.consumer_kind
    }

    pub fn polling_interval(&self) -> IggyDuration {
        self.polling_interval
    }

    pub fn polling_strategy(&self) -> PollingStrategy {
        self.polling_strategy
    }

    pub fn partitions_count(&self) -> u32 {
        self.partitions_count
    }

    pub fn replication_factor(&self) -> Option<u8> {
        self.replication_factor
    }
}
