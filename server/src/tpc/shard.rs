use super::{
    connector::{Receiver, ShardConnector, StopReceiver, StopSender},
    shard_frame::ShardFrame,
    utils::hash_string,
};
use flume::SendError;
pub const SHARD_NAME: &str = "iggy_shard";

pub struct IggyShard {
    pub shard_id: u16,
    pub hash: u32,
    pub message_receiver: Receiver<ShardFrame>,
    pub stop_receiver: StopReceiver,
    stop_sender: StopSender,

    shards: Vec<Shard>,
}

impl IggyShard {
    pub fn xd(id: u16, connections: Vec<ShardConnector<ShardFrame>>) -> Self {
        let (stop_sender, stop_receiver, receiver) = connections
            .iter()
            .filter(|c| c.id == id)
            .map(|c| {
                (
                    c.stop_sender.clone(),
                    c.stop_receiver.clone(),
                    c.receiver.clone(),
                )
            })
            .next()
            .unwrap();

        let shards = connections
            .into_iter()
            .map(|c| {
                let name = format!("{}_{}", SHARD_NAME, c.id);
                Shard::new(name, c)
            })
            .collect::<Vec<_>>();
        let shard_name = format!("{}_{}", SHARD_NAME, id);
        Self {
            shard_id: id,
            hash: hash_string(&shard_name).unwrap(),
            message_receiver: receiver,
            stop_receiver,
            stop_sender,
            shards,
        }
    }

    pub fn new(
        shard_id: u16,
        shards: Vec<Shard>,
        shard_messages_receiver: Receiver<ShardFrame>,
        stop_receiver: StopReceiver,
        stop_sender: StopSender,
    ) -> Self {
        let name = &format!("{}_{}", SHARD_NAME, shard_id);
        Self {
            shard_id,
            hash: hash_string(&name).unwrap(),
            message_receiver: shard_messages_receiver,
            stop_receiver,
            stop_sender,
            shards,
        }
    }

    pub async fn stop(self) -> Result<(), SendError<()>> {
        self.stop_sender.send_async(()).await?;
        Ok(())
    }
}

pub struct Shard {
    hash: u32,
    connection: ShardConnector<ShardFrame>,
}

impl Shard {
    pub fn new(name: String, connection: ShardConnector<ShardFrame>) -> Self {
        let hash = hash_string(&name).unwrap();
        Self { hash, connection }
    }
}
