use flume::SendError;

use super::{
    connector::{Receiver, ShardConnector, StopReceiver, StopSender},
    shard_frame::ShardFrame,
    utils::hash_string,
};
const SHARD_NAME: &str = "iggy_shard";

pub struct IggyShard {
    pub shard_id: u16,
    pub hash: u32,
    pub message_receiver: Receiver<ShardFrame>,
    pub stop_receiver: StopReceiver,
    stop_sender: StopSender,

    shards: Vec<Shard<ShardFrame>>,
}

impl IggyShard {
    pub fn new(id: u16, connections: Vec<ShardConnector<ShardFrame>>) -> Self {
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
        Self {
            shard_id: id,
            hash: hash_string(&format!("{}_{}", SHARD_NAME, id)).unwrap(),
            message_receiver: receiver,
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

pub struct Shard<T: Clone> {
    hash: u32,
    connection: ShardConnector<T>,
}

impl<T: Clone> Shard<T> {
    pub fn new(name: String, connection: ShardConnector<T>) -> Self {
        let hash = hash_string(&name).unwrap();
        Self {
            hash,
            connection,
        }
    }
}
