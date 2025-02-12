use crate::consumer_ext::MessageConsumer;
use crate::error::IggyError;
use async_trait::async_trait;
use tokio::sync::oneshot;

#[async_trait]
pub trait IggyConsumerMessageExt {
    async fn consume_messages(
        mut self,
        event_processor: &'static (impl MessageConsumer + Sync),
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), IggyError>;
}
