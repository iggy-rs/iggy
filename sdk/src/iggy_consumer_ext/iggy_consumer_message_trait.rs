use async_trait::async_trait;

use crate::error::IggyError;
use tokio::sync::oneshot;
use crate::iggy_consumer_ext::MessageConsumer;

#[async_trait]
pub trait IggyConsumerMessageExt {
    async fn consume_messages(
        mut self,
        event_processor: &'static (impl MessageConsumer + Sync),
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), IggyError>;
}
