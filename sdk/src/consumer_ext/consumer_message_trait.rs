use crate::consumer_ext::MessageConsumer;
use crate::error::IggyError;
use async_trait::async_trait;
use tokio::sync::oneshot;

#[async_trait]
pub trait IggyConsumerMessageExt {
    /// This function starts an event loop that consumes messages from the stream and
    /// applies the provided consumer. The loop will exit when the shutdown receiver is triggered.
    ///
    /// This can be combined with `AutoCommitAfter` to automatically commit offsets after consuming.
    ///
    /// # Arguments
    ///
    /// * `message_consumer`: The consumer to send messages to.
    /// * `shutdown_rx`: The receiver to listen to for shutdown.
    ///
    async fn consume_messages<P>(
        mut self,
        message_consumer: &'static P,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), IggyError>
    where
        P: MessageConsumer + Sync;
}
