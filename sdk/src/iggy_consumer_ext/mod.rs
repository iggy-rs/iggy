mod iggy_consumer_message_ext;
mod iggy_consumer_message_trait;

pub use iggy_consumer_message_trait::IggyConsumerMessageExt;

use crate::error::IggyError;
use crate::models::messages::PolledMessage;

/// Trait for event consumer
#[allow(dead_code)] // Clippy can't see that the trait is used
#[trait_variant::make(MessageConsumer: Send)]
pub trait LocalMessageConsumer {
    /// Consume a message from the message bus.
    ///
    /// # Arguments
    ///
    /// * `data` - The event data
    ///
    /// # Errors
    ///
    /// * `IggyError` - If the message consumer fails to consume the message
    async fn consume(&self, message: PolledMessage) -> Result<(), IggyError>;
}

// Default implementation for `&T`
// https://users.rust-lang.org/t/hashmap-get-dereferenced/33558
impl<T: MessageConsumer + Send + Sync> MessageConsumer for &T {
    /// Consume a message from the message bus.
    ///
    /// # Arguments
    ///
    /// * `data` - The event data
    ///
    /// # Errors
    ///
    /// * `IggyError` - If the message consumer fails to consume the message
    async fn consume(&self, message: PolledMessage) -> Result<(), IggyError> {
        (**self).consume(message).await
    }
}

