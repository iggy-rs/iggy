use iggy::command::Command;

use crate::tpc::connector::Receiver;

#[derive(Debug, Clone, Copy)]
pub enum ShardMessage {
    Request(Command),
    Event,
}

#[derive(Debug, Clone, Copy)]
pub enum ShardResponse {
    Response,
}

#[derive(Clone)]
pub struct ShardFrame {
    pub shard_id: u16,
    pub message: ShardMessage,
    pub response_receiver: Receiver<ShardResponse>,
}
