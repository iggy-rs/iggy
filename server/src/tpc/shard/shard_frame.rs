use crate::tpc::connector::Receiver;


#[derive(Clone)]
pub enum ShardMessage {
    Request,
}

#[derive(Clone)]
pub enum ShardResponse {
    Response,
}

#[derive(Clone)]
pub struct ShardFrame {
    pub shard_id: u16,
    pub message: ShardMessage,
    pub response_receiver: Receiver<ShardResponse>,
}
