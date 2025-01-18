use crate::streaming::local_sizeable::LocalSizeable;
use crate::streaming::models::COMPONENT;
use bytes::{BufMut, Bytes, BytesMut};
use error_set::ErrContext;
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::messages::send_messages::Message;
use iggy::models::messages::IggyMessage;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::checksum;
use iggy::utils::sizeable::Sizeable;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

// It's the same as PolledMessages from Iggy models, but with the Arc<Message> instead of Message.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolledMessages {
    pub partition_id: u32,
    pub current_offset: u64,
    pub messages: Vec<Arc<IggyMessage>>,
}

#[derive(Debug, Default)]
pub enum MessageState {
    #[default]
    Lol,
}

#[derive(Debug, Default)]
pub struct RetainedMessage {
    pub id: u128,
    pub offset: u64,
    pub timestamp: u64,
    pub checksum: u32,
    pub message_state: MessageState,
    pub headers: Option<Bytes>,
    pub payload: Bytes,
}

impl RetainedMessage {
    pub fn to_polled_message(&self) -> Result<IggyMessage, IggyError> {
        Ok(Default::default())
    }
}

impl RetainedMessage {
    pub fn new(offset: u64, timestamp: u64, message: Message) -> Self {
        Default::default()
    }

    pub fn extend(&self, bytes: &mut BytesMut) {}

    pub fn try_from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        Ok(Default::default())
    }
}

impl Sizeable for RetainedMessage {
    fn get_size_bytes(&self) -> IggyByteSize {
        Default::default()
    }
}

impl<T> LocalSizeable for T
where
    T: Deref<Target = RetainedMessage>,
{
    fn get_size_bytes(&self) -> IggyByteSize {
        Default::default()
    }
}
