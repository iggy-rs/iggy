use crate::clients::consumer::IggyConsumer;
use crate::error::IggyError;
use crate::iggy_consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
use async_trait::async_trait;
use futures_util::StreamExt;
use tokio::sync::oneshot;
use tracing::{error, info};

#[async_trait]
impl IggyConsumerMessageExt for IggyConsumer {
    /// Consume messages from the broker and process them with the given event processor
    ///
    /// # Arguments
    ///
    /// * `event_processor`: The event processor to use. This must be a reference to a static
    /// object that implements the `EventConsumer` trait.
    /// * `shutdown_rx`: A receiver which will receive a shutdown signal, which will be used to
    /// stop message consumption.
    ///
    /// # Errors
    ///
    /// * `IggyError::Disconnected`: The client has been disconnected.
    /// * `IggyError::CannotEstablishConnection`: The client cannot establish a connection to iggy.
    /// * `IggyError::StaleClient`: This client is stale and cannot be used to consume messages.
    /// * `IggyError::InvalidServerAddress`: The server address is invalid.
    /// * `IggyError::InvalidClientAddress`: The client address is invalid.
    /// * `IggyError::NotConnected`: The client is not connected.
    /// * `IggyError::ClientShutdown`: The client has been shut down.
    ///
    async fn consume_messages(
        mut self,
        event_processor: &'static (impl MessageConsumer + Sync),
        mut shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), IggyError> {
        loop {
            tokio::select! {
                // Check first if we have received a shutdown signal
                _ = &mut shutdown_rx => {
                    info!("Received shutdown signal, stopping message consumption");
                    break;
                }

                message = self.next() => {
                    match message {
                        Some(Ok(received_message)) => {
                            if let Err(err) = event_processor.consume(received_message.message).await {
                                error!("Error while handling message from delivered from consumer {name} on topic: {topic} and stream {stream} du to error {err}",
                                    name= self.name(), topic = self.topic(), stream = self.stream());
                            }
                        }
                        Some(Err(err)) => {
                            match err {
                                IggyError::Disconnected |
                                IggyError::CannotEstablishConnection |
                                IggyError::StaleClient |
                                IggyError::InvalidServerAddress |
                                IggyError::InvalidClientAddress |
                                IggyError::NotConnected |
                                IggyError::ClientShutdown => {
                                    error!("{err:?}: shutdown client: {err}");
                                    return Err(err);
                                }
                                _ => {
                                    error!("Error while handling message: {err}");
                                    continue;
                                }
                            }
                        }
                        None => break,
                    }
                }

            }
        }

        Ok(())
    }
}
