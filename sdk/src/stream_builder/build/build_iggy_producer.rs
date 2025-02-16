use crate::clients::client::IggyClient;
use crate::clients::producer::IggyProducer;
use crate::error::IggyError;
use crate::stream_builder::IggyProducerConfig;
use crate::utils::expiry::IggyExpiry;
use crate::utils::topic_size::MaxTopicSize;
use tracing::{error, trace};

/// Build a producer from the stream configuration.
///
/// # Arguments
///
/// * `client` - The Iggy client.
/// * `config` - The configuration.
///
/// # Errors
///
/// * `IggyError` - If the iggy producer cannot be build.
///
/// # Details
///
/// This function will create a new `IggyProducer` with the given `IggyClient` and `IggyProducerConfig`.
/// The `IggyProducerConfig` fields are used to configure the `IggyProducer`.
///
pub(crate) async fn build_iggy_producer(
    client: &IggyClient,
    config: &IggyProducerConfig,
) -> Result<IggyProducer, IggyError> {
    trace!("Extract config fields.");
    let stream = config.stream_name();
    let topic = config.topic_name();
    let topic_partitions_count = config.topic_partitions_count();
    let topic_replication_factor = config.topic_replication_factor();
    let batch_size = config.batch_size();
    let send_interval = config.send_interval();
    let partitioning = config.partitioning().to_owned();
    let send_retries = config.send_retries_count();
    let send_retries_interval = config.send_retries_interval();

    trace!("Build iggy producer");
    let mut builder = client
        .producer(stream, topic)?
        .batch_size(batch_size)
        .send_interval(send_interval)
        .partitioning(partitioning)
        .create_stream_if_not_exists()
        .send_retries(send_retries, send_retries_interval)
        .create_topic_if_not_exists(
            topic_partitions_count,
            topic_replication_factor,
            IggyExpiry::ServerDefault,
            MaxTopicSize::ServerDefault,
        );

    if let Some(encryptor) = config.encryptor() {
        builder = builder.encryptor(encryptor);
    }

    trace!("Initialize iggy producer");
    let mut producer = builder.build();
    producer.init().await.map_err(|err| {
        error!("Failed to initialize consumer: {err}");
        err
    })?;

    Ok(producer)
}
