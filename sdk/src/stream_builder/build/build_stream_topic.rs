use crate::client::{StreamClient, TopicClient};
use crate::clients::client::IggyClient;
use crate::compression::compression_algorithm::CompressionAlgorithm;
use crate::error::IggyError;
use crate::stream_builder::IggyConsumerConfig;
use crate::utils::expiry::IggyExpiry;
use crate::utils::topic_size::MaxTopicSize;
use tracing::{trace, warn};

/// Builds an `IggyStream` and `IggyTopic` if any of them does not exists
/// and if the boolean flags to create them are set to true. In that case it will build
/// them using the given `IggyClient` and `IggyProducerConfig`.
///
/// If the boolean flags to create them are set to false, it will not build them and will return
/// and return Ok(()) since this is expected behavior.d
///
/// # Arguments
///
/// * `client` - The `IggyClient` to use.
/// * `config` - The `IggyProducerConfig` to use.
///
/// # Errors
///
/// * `IggyError` - If the iggy stream topic cannot be build.
///
pub(crate) async fn build_iggy_stream_topic_if_not_exists(
    client: &IggyClient,
    config: &IggyConsumerConfig,
) -> Result<(), IggyError> {
    let stream_id = config.stream_id();
    let stream_name = config.stream_name();
    let topic_id = config.topic_id();
    let topic_name = config.topic_name();

    trace!("Check if stream exists.");
    if client.get_stream(config.stream_id()).await?.is_none() {
        trace!("Check if stream should be created.");
        if !config.create_stream_if_not_exists() {
            warn!(
                "Stream {stream_name} does not exists and create stream is disabled. \
                If you want to create the stream automatically, please set create_stream_if_not_exists to true."
            );
            return Ok(());
        }

        let (name, id) = stream_id.extract_name_id(stream_name.to_owned())?;
        trace!("Creating stream: {name}");
        client.create_stream(&name, id).await?;
    }

    trace!("Check if topic exists.");
    if client
        .get_topic(config.stream_id(), config.topic_id())
        .await?
        .is_none()
    {
        trace!("Check if topic should be created.");
        if !config.create_topic_if_not_exists() {
            warn!("Topic {topic_name} for stream {stream_name} does not exists and create topic is disabled.\
            If you want to create the topic automatically, please set create_topic_if_not_exists to true.");
            return Ok(());
        }

        let stream_id = config.stream_id();
        let stream_name = config.stream_name();
        let topic_partitions_count = config.partitions_count();
        let topic_replication_factor = config.replication_factor();

        let (name, id) = topic_id.extract_name_id(topic_name.to_owned())?;
        trace!("Create topic: {name} for stream: {}", stream_name);
        client
            .create_topic(
                stream_id,
                topic_name,
                topic_partitions_count,
                CompressionAlgorithm::None,
                topic_replication_factor,
                id,
                IggyExpiry::ServerDefault,
                MaxTopicSize::ServerDefault,
            )
            .await?;
    }

    Ok(())
}
