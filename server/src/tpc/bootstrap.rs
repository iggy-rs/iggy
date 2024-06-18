use futures::future::try_join_all;
use iggy::error::IggyError;
use monoio::task::JoinHandle;
use sled::Db;
use std::{rc::Rc, sync::Arc};

use crate::{
    configs::server::ServerConfig,
    streaming::{
        persistence::persister::{FilePersister, FileWithSyncPersister, StoragePersister},
        storage::SystemStorage,
    },
    tcp::tcp_server,
};

use super::{
    connector::ShardConnector,
    shard::{
        shard::{IggyShard, Shard, SHARD_NAME},
        shard_frame::ShardFrame,
    },
};

pub fn create_shard(
    id: u16,
    db: Arc<Db>,
    config: ServerConfig,
    connections: Vec<ShardConnector<ShardFrame>>,
) -> IggyShard {
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

    let persister = match config.system.partition.enforce_fsync {
        true => Arc::new(StoragePersister::FileWithSync(FileWithSyncPersister {})),
        false => Arc::new(StoragePersister::File(FilePersister {})),
    };
    let storage = Arc::new(SystemStorage::new(db.clone(), persister));
    IggyShard::new(
        id,
        shards,
        config,
        db,
        storage,
        receiver,
        stop_receiver,
        stop_sender,
    )
}

pub async fn shard_executor(shard: Rc<IggyShard>, is_prime_thread: bool) -> Result<(), IggyError> {
    // Workaround to ensure that the statistics are initialized before the server
    // loads streams and starts accepting connections. This is necessary to
    // have the correct statistics when the server starts.
    shard.get_stats_bypass_auth().await?;
    // TODO - make the init collections a Cell, so they can be mutated while being borrowed.
    shard.init().await?;
    // Create all tasks (tcp listener, http listener, command processor, in the future also the background handlers).
    let mut tasks = vec![];
    if shard.config.tcp.enabled {
        tasks.push(spawn_tcp_server(shard.clone()));
    }
    let result = try_join_all(tasks).await;
    // If its main thread, add to the list of joined tasks the task that will wait for the stop signal.
    // join_all all tasks, if it fails, then we can move to the graceful shutdown stage,

    // Write the toml config to disk only on one thread
    Ok(())
}

async fn spawn_tcp_server(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    tcp_server::start(shard).await
}
