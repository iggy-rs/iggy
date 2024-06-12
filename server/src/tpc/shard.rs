use super::{
    connector::{Receiver, ShardConnector},
    utils::hash_string,
};

pub struct IggyShard<T> {
    pub shard_id: u16,
    pub hash: u32,
    pub message_receiver: Receiver<T>,
    shards: Vec<Shard<T>>,
}

impl<T> IggyShard<T> {
    pub fn new(id: u16, receiver: Receiver<T>, connections: Vec<ShardConnector<T>>) {}
}

pub struct Shard<T> {
    hash: u32,
    connection: ShardConnector<T>,
}

impl<T> Shard<T> {
    pub fn new(name: String, connection: ShardConnector<T>) -> Self {
        let hash = hash_string(&name).unwrap();
        Self { hash, connection }
    }
}
