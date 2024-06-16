use std::sync::Arc;

use iggy::error::IggyError;

use crate::{
    configs::server::ServerConfig,
    streaming::{persistence::persister::{FilePersister, FileWithSyncPersister, StoragePersister}, storage::SystemStorage},
};

use super::{
    connector::ShardConnector, shard::{shard::{IggyShard, Shard, SHARD_NAME}, shard_frame::ShardFrame},
};

pub fn create_shard(
    id: u16,
    config: &ServerConfig,
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

    let system_config = config.system.clone();
    let pat_config = config.personal_access_token.clone();
    let db_path = system_config.get_database_path();
    let db = Arc::new(
        sled::open(&db_path).expect(format!("Cannot open database at: {}", db_path).as_ref()),
    );
    let persister = match system_config.partition.enforce_fsync {
        true => Arc::new(StoragePersister::FileWithSync(FileWithSyncPersister {})),
        false => Arc::new(StoragePersister::File(FilePersister {})),
    };
    let storage = Arc::new(SystemStorage::new(db.clone(), persister));
    IggyShard::new(
        id,
        shards,
        system_config,
        pat_config,
        db,
        storage,
        receiver,
        stop_receiver,
        stop_sender,
    )
}

pub async fn shard_executor(shard: IggyShard, is_main_shard: bool) -> Result<(), IggyError> {
    // Initialize system ?
    // Create all tasks (tcp listener, http listener, command processor, in the future also the background handlers).
    // If its main thread, add to the list of joined tasks the task that will wait for the stop signal.
    // join_all all tasks, if it fails, then we can move to the graceful shutdown stage,
    Ok(())
}
