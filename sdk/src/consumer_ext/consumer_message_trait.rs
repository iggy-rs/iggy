use crate::consumer_ext::MessageConsumer;
use crate::error::IggyError;
use async_trait::async_trait;
use tokio::sync::oneshot;

#[async_trait]
pub trait IggyConsumerMessageExt {
    /// This function starts an event loop that consumes messages from the stream and
    /// applies the provided processor. The loop will exit when the
    /// shutdown receiver is triggered.
    ///
    /// # Arguments
    ///
    /// * `event_processor`: The processor to send messages to.
    /// * `shutdown_rx`: The receiver to listen to for shutdown.
    ///
    async fn consume_messages(
        mut self,
        event_processor: &'static (impl MessageConsumer + Sync),
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), IggyError>;
}
