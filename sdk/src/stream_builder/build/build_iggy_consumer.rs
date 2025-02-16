use crate::clients::client::IggyClient;
use crate::clients::consumer::IggyConsumer;
use crate::consumer::ConsumerKind;
use crate::error::IggyError;
use crate::stream_builder::IggyConsumerConfig;
use tracing::{error, trace};

/// Builds an `IggyConsumer` from the given `IggyClient` and `IggyConsumerConfig`.
///
/// # Arguments
///
/// * `client` - The `IggyClient` to use.
/// * `config` - The `IggyConsumerConfig` to use.
///
/// # Errors
///
/// * `IggyError` - If the iggy consumer cannot be build.
///
/// # Details
///
/// This function will create a new `IggyConsumer` with the given `IggyClient` and `IggyConsumerConfig`.
/// The `IggyConsumerConfig` fields are used to configure the `IggyConsumer`.
///
pub(crate) async fn build_iggy_consumer(
    client: &IggyClient,
    config: &IggyConsumerConfig,
) -> Result<IggyConsumer, IggyError> {
    trace!("Extract config fields.");
    let stream = config.stream_name();
    let topic = config.topic_name();
    let auto_commit = config.auto_commit();
    let consumer_kind = config.consumer_kind();
    let consumer_name = config.consumer_name();
    let batch_size = config.batch_size();
    let polling_interval = config.polling_interval();
    let polling_strategy = config.polling_strategy();
    let partition = config.partitions_count();

    trace!("Build iggy consumer");
    let mut builder = match consumer_kind {
        ConsumerKind::Consumer => client.consumer(consumer_name, stream, topic, partition)?,
        ConsumerKind::ConsumerGroup => client.consumer_group(consumer_name, stream, topic)?,
    }
    .auto_commit(auto_commit)
    .create_consumer_group_if_not_exists()
    .auto_join_consumer_group()
    .polling_strategy(polling_strategy)
    .poll_interval(polling_interval)
    .batch_size(batch_size);

    if let Some(encryptor) = config.encryptor() {
        trace!("Set encryptor");
        builder = builder.encryptor(encryptor);
    }

    trace!("Initialize consumer");
    let mut consumer = builder.build();
    consumer.init().await.map_err(|err| {
        error!("Failed to initialize consumer: {err}");
        err
    })?;

    Ok(consumer)
}
