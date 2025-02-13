use crate::config::IggyConsumerConfig;
use crate::config::IggyProducerConfig;
use crate::identifier::Identifier;
use crate::utils::duration::IggyDuration;
use bon::Builder;

#[derive(Builder, Debug, Clone)]
pub struct IggyStreamConfig {
    consumer_config: IggyConsumerConfig,
    producer_config: IggyProducerConfig,
}

impl Default for IggyStreamConfig {
    fn default() -> Self {
        Self {
            consumer_config: IggyConsumerConfig::default(),
            producer_config: IggyProducerConfig::default(),
        }
    }
}

impl IggyStreamConfig {
    /// Creates a new `IggyStreamConfig` with the given consumer and producer configurations.
    ///
    /// # Args
    ///
    /// * `consumer_config` - The consumer configuration.
    /// * `producer_config` - The producer configuration.
    ///
    /// Returns:
    /// A new `IggyStreamConfig`.
    ///
    pub fn new(consumer_config: IggyConsumerConfig, producer_config: IggyProducerConfig) -> Self {
        Self {
            consumer_config,
            producer_config,
        }
    }

    /// Creates a new `IggyStreamConfig` from the given stream and topic names, along with the max
    /// batch size, the send interval and the polling interval.
    ///
    /// # Args
    ///
    /// * `stream` - The stream name.
    /// * `topic` - The topic name.
    /// * `batch_size` - The max number of messages to send in a batch.
    /// * `send_interval` - The interval between messages sent.
    /// * `polling_interval` - The interval between polling for new messages.
    ///
    /// Returns:
    /// A new `IggyStreamConfig`.
    ///
    pub fn from_stream_topic(
        stream: &str,
        topic: &str,
        batch_size: u32,
        send_interval: IggyDuration,
        polling_interval: IggyDuration,
    ) -> Self {
        let consumer_config =
            IggyConsumerConfig::from_stream_topic(stream, topic, batch_size, polling_interval);

        let producer_config =
            IggyProducerConfig::from_stream_topic(stream, topic, batch_size, send_interval);

        Self {
            consumer_config,
            producer_config,
        }
    }
}

impl IggyStreamConfig {
    pub fn consumer_config(&self) -> &IggyConsumerConfig {
        &self.consumer_config
    }

    pub fn producer_config(&self) -> &IggyProducerConfig {
        &self.producer_config
    }

    pub fn stream_id(&self) -> &Identifier {
        &self.producer_config.stream_id()
    }

    pub fn stream_name(&self) -> &str {
        &self.producer_config.stream_name()
    }

    pub fn topic_id(&self) -> &Identifier {
        &self.producer_config.topic_id()
    }

    pub fn topic_name(&self) -> &str {
        &self.producer_config.topic_name()
    }
}
