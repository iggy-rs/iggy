use futures::future::{join, try_join, try_join_all};
use futures::Future;
use iggy::error::IggyError;
use local_sync::semaphore::Semaphore;
use sled::Db;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::Mutex;
use std::{rc::Rc, sync::Arc};

use crate::streaming::persistence::persister::PersistenceStorage;
use crate::tcp::tcp_server::spawn_tcp_server;
use crate::tcp::{tcp_listener, tcp_tls_listener};
use crate::{configs::server::ServerConfig, streaming::storage::SystemStorage, tcp::tcp_server};

use super::shard::tasks::message_task::spawn_shard_message_task;
use super::{
    connector::ShardConnector,
    shard::{
        shard::{IggyShard, Shard, SHARD_NAME},
        shard_frame::ShardFrame,
    },
};
type Task = Pin<Box<dyn Future<Output = Result<(), IggyError>>>>;

pub fn create_shard(
    id: u16,
    init_gate: Arc<Mutex<()>>,
    config: ServerConfig,
    connections: Vec<ShardConnector<ShardFrame>>,
) -> Rc<IggyShard> {
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
        .map(|c| Shard::new(c.id, c))
        .collect::<Vec<_>>();

    let persister = match config.system.partition.enforce_fsync {
        true => Rc::new(PersistenceStorage::FileWithSync),
        false => Rc::new(PersistenceStorage::File),
    };
    let storage = Rc::new(SystemStorage::new(config.system.clone(), persister.clone()));
    Rc::new(IggyShard::new(
        id,
        shards,
        init_gate,
        config,
        storage,
        persister,
        receiver,
        stop_receiver,
        stop_sender,
    ))
}

pub async fn shard_executor(shard: Rc<IggyShard>, is_prime_thread: bool) -> Result<(), IggyError> {
    // Workaround to ensure that the statistics are initialized before the server
    // loads streams and starts accepting connections. This is necessary to
    // have the correct statistics when the server starts.
    shard.get_stats_bypass_auth().await?;
    shard.init(is_prime_thread).await?;
    // Create all tasks (tcp listener, http listener, command processor, in the future also the background jobs).
    let mut tasks: Vec<Task> = vec![Box::pin(spawn_shard_message_task(shard.clone()))];
    if shard.config.tcp.enabled {
        tasks.push(Box::pin(spawn_tcp_server(shard.clone())));
    }
    let result = try_join_all(tasks).await;
    result?;
    // If its main thread, add to the list of joined tasks the task that will wait for the stop signal.
    // join_all all tasks, if it fails, then we can move to the graceful shutdown stage,

    Ok(())
}
