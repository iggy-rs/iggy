use crate::client::SystemClient;
use crate::clients::client::IggyClient;
use crate::clients::producer::IggyProducer;
use crate::error::IggyError;
use crate::stream_builder::{build, IggyProducerConfig};
use tracing::trace;

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct IggyStreamProducer;

impl IggyStreamProducer {
    /// Creates a new `IggyProducer` instance and its associated producer using the `client` and
    /// `config` parameters.
    ///
    /// # Arguments
    ///
    /// * `client`: The Iggy client to use to connect to the Iggy server.
    /// * `config`: The configuration for the producer.
    ///
    /// # Errors
    ///
    /// If the client is not connected or the producer cannot be built, an `IggyError` is returned.
    ///
    pub async fn build(
        client: &IggyClient,
        config: &IggyProducerConfig,
    ) -> Result<IggyProducer, IggyError> {
        trace!("Check if client is connected");
        if client.ping().await.is_err() {
            return Err(IggyError::NotConnected);
        }

        trace!("Build iggy producer");
        // The producer creates stream and topic if it doesn't exist
        let iggy_producer = build::build_iggy_producer(client, config).await?;

        Ok(iggy_producer)
    }

    /// Creates a new `IggyStreamProducer` instance and its associated client using the `connection_string`
    /// and `config` parameters.
    ///
    /// # Arguments
    ///
    /// * `connection_string`: The connection string to use to connect to the Iggy server.
    /// * `config`: The configuration for the producer.
    ///
    /// # Errors
    ///
    /// If the client cannot be connected or the producer cannot be built, an `IggyError` is returned.
    ///
    pub async fn with_client_from_url(
        connection_string: &str,
        config: &IggyProducerConfig,
    ) -> Result<(IggyClient, IggyProducer), IggyError> {
        trace!("Build and connect iggy client");
        let client = build::build_iggy_client::build_iggy_client(connection_string).await?;

        trace!("Build iggy producer");
        // The producer creates stream and topic if it doesn't exist
        let iggy_producer = build::build_iggy_producer(&client, config).await?;

        Ok((client, iggy_producer))
    }
}
