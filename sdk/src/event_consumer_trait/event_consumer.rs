use crate::event_consumer_trait::EventConsumerError;
use crate::models::messages::PolledMessage;

/// Trait for event consumer
#[allow(dead_code)] // Clippy can't see that the trait is used
#[trait_variant::make(EventConsumer: Send)]
pub trait LocalEventConsumer {
    /// Consume a event from the message bus.
    ///
    /// # Arguments
    ///
    /// * `data` - The event data
    ///
    /// # Errors
    ///
    /// * `EventConsumerError` - If the event consumer fails to consume the event
    async fn consume(&self, message: PolledMessage) -> Result<(), EventConsumerError>;
}

// Default implementation for `&T`
// https://users.rust-lang.org/t/hashmap-get-dereferenced/33558
impl<T: EventConsumer + Send + Sync> EventConsumer for &T {
    /// Consume a event from the message bus.
    ///
    /// # Arguments
    ///
    /// * `data` - The event data
    ///
    /// # Errors
    ///
    /// * `EventConsumerError` - If the event consumer fails to consume the event
    async fn consume(&self, message: PolledMessage) -> Result<(), EventConsumerError> {
        (**self).consume(message).await
    }
}
