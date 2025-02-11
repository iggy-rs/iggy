use async_trait::async_trait;
use futures_util::StreamExt;

use crate::clients::consumer::IggyConsumer;
use crate::error::IggyError;
use crate::event_consumer_trait::EventConsumer;
use crate::iggy_consumer_ext::IggyConsumerMessageExt;
use tokio::sync::oneshot;
use tracing::{error, info};

#[async_trait]
impl IggyConsumerMessageExt for IggyConsumer {
    async fn consume_messages(
        mut self,
        event_processor: &'static (impl EventConsumer + Sync),
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
                                error!("Error while handling message: {err}");
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

        // We have to wait for a moment for the iggy server.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        Ok(())
    }
}
