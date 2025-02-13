use crate::client::{StreamClient, TopicClient};
use crate::clients::client::IggyClient;
use crate::compression::compression_algorithm::CompressionAlgorithm;
use crate::error::IggyError;
use crate::identifier::{IdKind, Identifier};
use crate::stream_config::IggyConsumerConfig;
use crate::utils::expiry::IggyExpiry;
use crate::utils::topic_size::MaxTopicSize;
use tracing::info;

/// Builds an `IggyStream` and `IggyTopic` if any of them does not exists
/// using the given `IggyClient` and `IggyProducerConfig`.
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
    info!("Check if stream exists.");
    if client.get_stream(config.stream_id()).await?.is_none() {
        let stream_id = config.stream_id();
        let stream_name = config.stream_name();

        let (name, id) = extract_name_id_from_identifier(stream_id, stream_name)?;
        info!("Creating stream: {name}");
        client.create_stream(&name, id).await?;
    }

    info!("Check if topic exists.");
    if client
        .get_topic(config.stream_id(), config.topic_id())
        .await?
        .is_none()
    {
        let stream_id = config.stream_id();
        let stream_name = config.stream_name();
        let topic_id = config.topic_id();
        let topic_name = config.topic_name();
        let partitions_count = config.partitions_count();
        let replication_factor = config.replication_factor();

        let (name, id) = extract_name_id_from_identifier(topic_id, topic_name)?;
        info!("Creating topic: {name} for stream: {}", stream_name);
        client
            .create_topic(
                stream_id,
                topic_name,
                partitions_count,
                CompressionAlgorithm::None,
                replication_factor,
                id,
                IggyExpiry::ServerDefault,
                MaxTopicSize::ServerDefault,
            )
            .await?;
    }

    Ok(())
}

fn extract_name_id_from_identifier(
    stream_id: &Identifier,
    stream_name: &str,
) -> Result<(String, Option<u32>), IggyError> {
    let (name, id) = match stream_id.kind {
        IdKind::Numeric => (stream_name.to_owned(), Some(stream_id.get_u32_value()?)),
        IdKind::String => (stream_id.get_string_value()?, None),
    };
    Ok((name, id))
}
