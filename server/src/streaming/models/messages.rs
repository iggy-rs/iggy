use iggy::models::messages::Message;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// It's the same as PolledMessages from Iggy models, but with the Arc<Message> instead of Message.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolledMessages {
    pub partition_id: u32,
    pub current_offset: u64,
    pub messages: Vec<Arc<Message>>,
}
