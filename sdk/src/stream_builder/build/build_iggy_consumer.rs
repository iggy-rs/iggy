use crate::clients::client::IggyClient;
use crate::clients::consumer::IggyConsumer;
use crate::consumer::ConsumerKind;
use crate::error::IggyError;
use crate::stream_config::IggyConsumerConfig;
use tracing::{error, info};

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
    info!("Extract config fields.");
    let stream = config.stream_name();
    let topic = config.topic_name();
    let auto_commit = config.auto_commit();
    let consumer_kind = config.consumer_kind();
    let consumer_name = config.consumer_name();
    let batch_size = config.batch_size();
    let polling_interval = config.polling_interval();
    let polling_strategy = config.polling_strategy();
    let partition = config.partitions_count();
    // let encryptor = config.encryptor().to_owned().unwrap();

    info!("Build iggy consumer");
    let mut consumer = match consumer_kind {
        ConsumerKind::Consumer => client.consumer(consumer_name, stream, topic, partition)?,
        ConsumerKind::ConsumerGroup => client.consumer_group(consumer_name, stream, topic)?,
    }
    .auto_commit(auto_commit)
    .create_consumer_group_if_not_exists()
    .auto_join_consumer_group()
    .polling_strategy(polling_strategy)
    .poll_interval(polling_interval)
    .batch_size(batch_size)
    .build();

    info!("Initialize producer");
    match consumer.init().await {
        Ok(_) => {}
        Err(err) => {
            error!("Failed to initialize consumer: {}", err);
            return Err(err);
        }
    }

    Ok(consumer)
}
