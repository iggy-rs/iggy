use async_trait::async_trait;

use crate::error::IggyError;
use crate::event_consumer_trait::EventConsumer;
use tokio::sync::oneshot;

#[async_trait]
pub trait IggyConsumerMessageExt {
    async fn consume_messages(
        mut self,
        event_processor: &'static (impl EventConsumer + Sync),
        shutdown_rx: oneshot::Receiver<()>, // or any `Future<Output=()>`
    ) -> Result<(), IggyError>;
}
