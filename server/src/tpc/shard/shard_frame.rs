use async_channel::Sender;
use bytes::Bytes;
use iggy::command::Command;
use iggy::error::IggyError;

#[derive(Debug, Clone)]
pub enum ShardMessage {
    Command(Command),
    Event,
}

#[derive(Debug)]
pub enum ShardResponse {
    BinaryResponse(Bytes),
    ErrorResponse(IggyError),
}

#[derive(Debug, Clone)]
pub struct ShardFrame {
    pub message: ShardMessage,
    pub response_sender: Sender<ShardResponse>,
}

impl ShardFrame {
    pub fn new(message: ShardMessage, response_sender: Sender<ShardResponse>) -> Self {
        Self {
            message,
            response_sender,
        }
    }
}
