use std::{collections::HashMap, sync::Arc};

use crate::{
    configs::{server::PersonalAccessTokenConfig, system::SystemConfig},
    streaming::{
        clients::client_manager::ClientManager, diagnostics::metrics::Metrics, persistence::persister::StoragePersister, storage::SystemStorage, streams::stream::Stream, users::permissioner::Permissioner
    }, tpc::{connector::{Receiver, ShardConnector, StopReceiver, StopSender}, utils::hash_string},
};

use super::{
    shard_frame::ShardFrame,
};
use flume::SendError;
use iggy::{locking::{IggySharedMut, IggySharedMutFn}, utils::crypto::{Aes256GcmEncryptor, Encryptor}};
use sled::Db;
pub const SHARD_NAME: &str = "iggy_shard";

pub struct IggyShard {
    pub id: u16,
    pub hash: u32,
    shards: Vec<Shard>,

    permissioner: Permissioner,
    storage: Arc<SystemStorage>,
    streams: HashMap<u32, Stream>,
    streams_ids: HashMap<String, u32>,
    encryptor: Option<Box<dyn Encryptor>>,
    system_config: Arc<SystemConfig>,
    client_manager: IggySharedMut<ClientManager>,
    // TODO - remove this dynamic dispatch
    metrics: Metrics,
    db: Arc<Db>,
    pat_config: PersonalAccessTokenConfig,
    message_receiver: Receiver<ShardFrame>,
    stop_receiver: StopReceiver,
    stop_sender: StopSender,
}

impl IggyShard {
    pub fn new(
        id: u16,
        shards: Vec<Shard>,
        system_config: Arc<SystemConfig>,
        pat_config: PersonalAccessTokenConfig,
        db: Arc<Db>,
        storage: Arc<SystemStorage>,
        shard_messages_receiver: Receiver<ShardFrame>,
        stop_receiver: StopReceiver,
        stop_sender: StopSender,
    ) -> Self {
        let name = &format!("{}_{}", SHARD_NAME, id);
        Self {
            id,
            hash: hash_string(&name).unwrap(),
            shards,
            permissioner: Permissioner::default(),
            storage,
            streams: HashMap::new(),
            streams_ids: HashMap::new(),
            encryptor: match system_config.encryption.enabled {
                true => Some(Box::new(
                    Aes256GcmEncryptor::from_base64_key(&system_config.encryption.key).unwrap(),
                )),
                false => None,
            },
            system_config,
            client_manager: IggySharedMut::new(ClientManager::default()),
            metrics: Metrics::init(),
            db,
            pat_config,
            message_receiver: shard_messages_receiver,
            stop_receiver,
            stop_sender,
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
