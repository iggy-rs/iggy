mod consumer_message_ext;
mod consumer_message_trait;

use crate::clients::consumer::ReceivedMessage;
use crate::error::IggyError;
pub use consumer_message_trait::IggyConsumerMessageExt;

/// Trait for message consumer
#[allow(dead_code)] // Clippy can't see that the trait is used
#[trait_variant::make(MessageConsumer: Send)]
pub trait LocalMessageConsumer {
    /// Consume a message from the message bus.
    ///
    /// # Arguments
    ///
    /// * `message` - The received message to consume
    ///
    /// # Errors
    ///
    /// * `IggyError` - If the message consumer fails to consume the message
    async fn consume(&self, message: ReceivedMessage) -> Result<(), IggyError>;
}

// Default implementation for `&T`
// https://users.rust-lang.org/t/hashmap-get-dereferenced/33558
impl<T: MessageConsumer + Send + Sync> MessageConsumer for &T {
    /// Consume a message from the message bus.
    ///
    /// # Arguments
    ///
    /// * `message` - The received message to consume
    ///
    /// # Errors
    ///
    /// * `IggyError` - If the message consumer fails to consume the message
    async fn consume(&self, message: ReceivedMessage) -> Result<(), IggyError> {
        (**self).consume(message).await
    }
}
