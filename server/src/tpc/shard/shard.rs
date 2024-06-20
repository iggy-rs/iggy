use std::{
    collections::HashMap,
    fs::{create_dir, remove_dir_all},
    path::Path,
    sync::{Arc, OnceLock},
    time::Instant,
};
use sysinfo::{Pid, System as SysinfoSystem};
use tracing::info;

use crate::{
    configs::{
        server::{PersonalAccessTokenConfig, ServerConfig},
        system::SystemConfig,
        tcp::TcpConfig,
    },
    streaming::{
        cache::memory_tracker::CacheMemoryTracker, clients::client_manager::ClientManager,
        diagnostics::metrics::Metrics, persistence::persister::StoragePersister, session::Session,
        storage::SystemStorage, streams::stream::Stream, users::permissioner::Permissioner,
    },
    tpc::{
        connector::{Receiver, ShardConnector, StopReceiver, StopSender},
        utils::hash_string,
    },
};

use super::shard_frame::ShardFrame;
use fast_async_mutex::mutex::Mutex;
use flume::SendError;
use iggy::{
    error::IggyError,
    locking::{IggySharedMut, IggySharedMutFn},
    models::stats::Stats,
    utils::crypto::{Aes256GcmEncryptor, Encryptor},
};
use sled::Db;
pub const SHARD_NAME: &str = "iggy_shard";

/// For each cache eviction, we want to remove more than the size we need.
/// This is done on purpose to avoid evicting messages on every write.
const CACHE_OVER_EVICTION_FACTOR: u64 = 5;

pub struct IggyShard {
    pub id: u16,
    pub hash: u32,
    shards: Vec<Shard>,

    pub(crate) permissioner: Permissioner,
    pub(crate) storage: Arc<SystemStorage>,
    pub(crate) streams: HashMap<u32, Stream>,
    pub(crate) streams_ids: HashMap<String, u32>,
    // TODO - get rid of this dynamic dispatch.
    pub(crate) encryptor: Option<Box<dyn Encryptor>>,
    pub(crate) config: ServerConfig,
    pub(crate) client_manager: IggySharedMut<ClientManager>,
    pub(crate) metrics: Metrics,
    db: Arc<Db>,
    message_receiver: Receiver<ShardFrame>,
    stop_receiver: StopReceiver,
    stop_sender: StopSender,
}

impl IggyShard {
    pub fn new(
        id: u16,
        shards: Vec<Shard>,
        config: ServerConfig,
        db: Arc<Db>,
        storage: Arc<SystemStorage>,
        shard_messages_receiver: Receiver<ShardFrame>,
        stop_receiver: StopReceiver,
        stop_sender: StopSender,
    ) -> Self {
        let name = &format!("{}_{}", SHARD_NAME, id);
        let this = Self {
            id,
            hash: hash_string(&name).unwrap(),
            shards,
            permissioner: Permissioner::default(),
            storage,
            streams: HashMap::new(),
            streams_ids: HashMap::new(),
            encryptor: match config.system.encryption.enabled {
                true => Some(Box::new(
                    Aes256GcmEncryptor::from_base64_key(&config.system.encryption.key).unwrap(),
                )),
                false => None,
            },
            config,
            client_manager: IggySharedMut::new(ClientManager::default()),
            metrics: Metrics::init(),
            db,
            message_receiver: shard_messages_receiver,
            stop_receiver,
            stop_sender,
        };
        // For now like this, TODO - make the Vec<Shards> a Cell.
        this.sort_consistent_hash_ring()
    }

    pub async fn init(&mut self) -> Result<(), IggyError> {
        info!(
            "Initializing system, data will be stored at: {}",
            self.config.system.get_system_path()
        );
        let now = Instant::now();
        self.load_version().await?;
        self.load_users().await?;
        self.load_streams().await?;
        info!("Initialized system in {} ms.", now.elapsed().as_millis());
        Ok(())
    }

    fn sort_consistent_hash_ring(mut self) -> Self {
        self.shards.sort_unstable_by(|a, b| {
            let x = a.hash;
            let y = b.hash;
            let threshold = self.hash;
            if x < threshold && y >= threshold {
                std::cmp::Ordering::Greater
            } else if x >= threshold && y < threshold {
                std::cmp::Ordering::Less
            } else {
                x.cmp(&y)
            }
        });
        self
    }

    pub async fn stop(self) -> Result<(), SendError<()>> {
        self.stop_sender.send_async(()).await?;
        Ok(())
    }

    pub fn ensure_authenticated(&self, session: &Session) -> Result<(), IggyError> {
        match session.is_authenticated() {
            true => Ok(()),
            false => Err(IggyError::Unauthenticated),
        }
    }

    fn map_toggle_str<'a>(enabled: bool) -> &'a str {
        match enabled {
            true => "enabled",
            false => "disabled",
        }
    }

    pub async fn clean_cache(&self, size_to_clean: u64) {
        for stream in self.streams.values() {
            for topic in stream.get_topics() {
                for partition in topic.get_partitions().into_iter() {
                    monoio::spawn(async move {
                        let memory_tracker = CacheMemoryTracker::get_instance().unwrap();
                        let mut partition_guard = partition.write().await;
                        let cache = &mut partition_guard.cache.as_mut().unwrap();
                        let size_to_remove = (cache.current_size() as f64
                            / memory_tracker.usage_bytes() as f64
                            * size_to_clean as f64)
                            .ceil() as u64;
                        cache.evict_by_size(size_to_remove * CACHE_OVER_EVICTION_FACTOR);
                    });
                }
            }
        }
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
