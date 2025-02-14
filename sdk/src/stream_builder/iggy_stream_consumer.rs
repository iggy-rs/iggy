use crate::client::SystemClient;
use crate::clients::client::IggyClient;
use crate::clients::consumer::IggyConsumer;
use crate::error::IggyError;
use crate::stream_builder::build;
use crate::stream_config::IggyConsumerConfig;
use tracing::info;
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct IggyStreamConsumer {}

impl IggyStreamConsumer {
    /// Creates a new `IggyStreamConsumer` with an existing client and `IggyConsumerConfig`.
    ///
    /// # Arguments
    ///
    /// * `client`: the existing `IggyClient` to use for the consumer.
    /// * `config`: the `IggyConsumerConfig` to use to build the consumer.
    ///
    /// # Errors
    ///
    /// If the builds fails, an `IggyError` is returned.
    ///
    pub async fn build(
        client: &IggyClient,
        config: &IggyConsumerConfig,
    ) -> Result<IggyConsumer, IggyError> {
        info!("Check if client is connected");
        if client.ping().await.is_err() {
            return Err(IggyError::NotConnected);
        }

        info!("Check if stream and topic exist");
        match build::build_iggy_stream_topic_if_not_exists(client, config).await {
            Ok(_) => (),
            Err(err) => return Err(err),
        }

        info!("Build iggy consumer");
        let iggy_consumer =
            match build::build_iggy_consumer::build_iggy_consumer(client, config).await {
                Ok(iggy_consumer) => iggy_consumer,
                Err(err) => return Err(err),
            };

        Ok(iggy_consumer)
    }

    /// Creates a new `IggyStreamConsumer` by building a client from a connection string and
    /// a consumer with an `IggyConsumerConfig`.
    ///
    /// # Arguments
    ///
    /// * `connection_string`: the connection string to use to build the client.
    /// * `config`: the `IggyConsumerConfig` to use to build the consumer.
    ///
    /// # Errors
    ///
    /// If the builds fails, an `IggyError` is returned.
    ///
    pub async fn with_client_from_url(
        connection_string: &str,
        config: &IggyConsumerConfig,
    ) -> Result<(IggyClient, IggyConsumer), IggyError> {
        info!("Build and connect iggy client");
        let client = build::build_iggy_client(connection_string).await?;

        info!("Check if stream and topic exist");
        match build::build_iggy_stream_topic_if_not_exists(&client, config).await {
            Ok(_) => (),
            Err(err) => return Err(err),
        }

        info!("Build iggy consumer");
        let iggy_consumer = match build::build_iggy_consumer(&client, config).await {
            Ok(iggy_consumer) => iggy_consumer,
            Err(err) => return Err(err),
        };

        Ok((client, iggy_consumer))
    }
}
