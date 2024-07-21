use super::shard_frame::{ShardEvent, ShardFrame, ShardMessage, ShardResponse};
use crate::state::State;
use crate::streaming::persistence::persister::PersistenceStorage;
use crate::{
    command::ServerCommand,
    configs::server::ServerConfig,
    state::{file::FileState, system::SystemState},
    streaming::{
        cache::memory_tracker::CacheMemoryTracker,
        clients::client_manager::ClientManager,
        diagnostics::metrics::Metrics,
        session::Session,
        storage::SystemStorage,
        streams::stream::Stream,
        users::{permissioner::Permissioner, user::User},
    },
    tpc::connector::{Receiver, ShardConnector, StopReceiver, StopSender},
    versioning::SemanticVersion,
};
use clap::error;
use flume::SendError;
use iggy::models::{resource_namespace::IggyResourceNamespace, user_info::UserId};
use iggy::{
    error::IggyError,
    utils::crypto::{Aes256GcmEncryptor, Encryptor},
};
use local_sync::semaphore::Semaphore;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};
use std::{collections::HashMap, time::Instant};
use tracing::{error, info};

pub const SHARD_NAME: &str = "iggy_shard";

/// For each cache eviction, we want to remove more than the size we need.
/// This is done on purpose to avoid evicting messages on every write.
const CACHE_OVER_EVICTION_FACTOR: u64 = 5;

pub struct Shard {
    id: u16,
    connection: ShardConnector<ShardFrame>,
}

impl Shard {
    pub fn new(id: u16, connection: ShardConnector<ShardFrame>) -> Self {
        Self { id, connection }
    }

    pub async fn send_request(
        &self,
        client_id: u32,
        message: ShardMessage,
    ) -> Result<ShardResponse, IggyError> {
        let (sender, receiver) = async_channel::bounded(1);
        self.connection
            .sender
            .send(ShardFrame::new(client_id, message, Some(sender.clone()))); // Apparently sender needs to be cloned, otherwise channel will close...
        let response = receiver.recv().await?;
        Ok(response)
    }
}

#[derive(Debug)]
pub struct ShardInfo {
    pub(crate) id: u16,
    // TODO - this will be extended with an revision number, once replication is implemented.
}

pub struct IggyShard {
    pub id: u16,
    shards: Vec<Shard>,
    shards_table: RefCell<HashMap<IggyResourceNamespace, ShardInfo>>,
    init_gate: Arc<Mutex<()>>,

    pub(crate) permissioner: RefCell<Permissioner>,
    pub(crate) storage: Rc<SystemStorage>,
    pub(crate) streams: RefCell<HashMap<u32, Stream>>,
    pub(crate) streams_ids: RefCell<HashMap<String, u32>>,
    pub(crate) users: RefCell<HashMap<UserId, User>>,

    // TODO - get rid of this dynamic dispatch.
    pub(crate) state: Rc<FileState>,
    pub(crate) encryptor: Option<Rc<dyn Encryptor>>,
    pub(crate) config: ServerConfig,
    pub(crate) client_manager: RefCell<ClientManager>,
    pub(crate) active_sessions: RefCell<Vec<Session>>,
    pub(crate) metrics: Metrics,
    pub message_receiver: Cell<Option<Receiver<ShardFrame>>>,
    stop_receiver: StopReceiver,
    stop_sender: StopSender,
}
impl IggyShard {
    pub fn new(
        id: u16,
        shards: Vec<Shard>,
        init_gate: Arc<Mutex<()>>,
        config: ServerConfig,
        storage: Rc<SystemStorage>,
        persister: Rc<PersistenceStorage>,
        shard_messages_receiver: Receiver<ShardFrame>,
        stop_receiver: StopReceiver,
        stop_sender: StopSender,
    ) -> Self {
        let version = SemanticVersion::current().expect("Invalid version");
        Self {
            id,
            shards,
            shards_table: RefCell::new(HashMap::new()),
            init_gate,
            permissioner: RefCell::new(Permissioner::default()),
            storage,
            streams: RefCell::new(HashMap::new()),
            users: RefCell::new(HashMap::new()),
            streams_ids: RefCell::new(HashMap::new()),
            encryptor: match config.system.encryption.enabled {
                true => Some(Rc::new(
                    Aes256GcmEncryptor::from_base64_key(&config.system.encryption.key).unwrap(),
                )),
                false => None,
            },
            state: Rc::new(FileState::new(
                &config.system.get_state_log_path(),
                &version,
                persister,
                match config.system.encryption.enabled {
                    true => Some(Rc::new(
                        Aes256GcmEncryptor::from_base64_key(&config.system.encryption.key).unwrap(),
                    )),
                    false => None,
                },
            )),
            config,
            client_manager: RefCell::new(ClientManager::default()),
            active_sessions: RefCell::new(Vec::new()),
            metrics: Metrics::init(),
            message_receiver: Cell::new(Some(shard_messages_receiver)),
            stop_receiver,
            stop_sender,
        }
    }

    pub async fn init(&self, is_prime_thread: bool) -> Result<(), IggyError> {
        info!(
            "Initializing system, data will be stored at: {}",
            self.config.system.get_system_path()
        );
        let now = Instant::now();
        // Not the cleanest way to do this, it has could be improved later on
        // (move the creation of state dirs and root user before shards initialization.).
        let guard = self.init_gate.lock().unwrap();
        let state_entries = self.state.init().await?;
        let system_state = SystemState::init(state_entries).await?;
        self.load_version().await?;
        self.load_users(system_state.users.into_values().collect())
            .await?;
        self.load_streams(system_state.streams.into_values().collect())
            .await?;
        drop(guard);
        /*
        if let Some(archiver) = self.archiver.as_ref() {
            archiver
                .init()
                .await
                .expect("Failed to initialize archiver");
        }
        */
        info!("Initialized system in {} ms.", now.elapsed().as_millis());
        Ok(())
    }

    pub fn get_available_shards_count(&self) -> u32 {
        self.shards.len() as u32
    }

    pub async fn stop(self) -> Result<(), SendError<()>> {
        self.stop_sender.send_async(()).await?;
        Ok(())
    }

    pub fn add_active_session(&self, session: Session) {
        let mut active_sessions = self.active_sessions.borrow_mut();
        active_sessions.push(session);
    }

    pub fn ensure_authenticated(&self, client_id: u32) -> Result<u32, IggyError> {
        let active_sessions = self.active_sessions.borrow();
        let session = active_sessions
            .iter()
            .find(|s| s.client_id == client_id)
            .ok_or_else(|| IggyError::Unauthenticated)?;
        session
            .is_authenticated()
            .and_then(|_| Ok(session.get_user_id()))
    }

    pub fn broadcast_event_to_all_shards(&self, client_id: u32, event: ShardEvent) {
        let connections = self
            .shards
            .iter()
            .filter_map(|shard| {
                if shard.id != self.id {
                    Some(shard.connection.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let _ = connections
            .iter()
            .map(|connection| {
                let message = ShardMessage::Event(event.clone());
                connection.send(ShardFrame::new(client_id, message, None));
            })
            .collect::<Vec<_>>();
    }

    pub async fn send_request_to_shard(
        &self,
        client_id: u32,
        resource_ns: IggyResourceNamespace,
        command: ServerCommand,
    ) -> Result<ShardResponse, IggyError> {
        let shard = self.find_shard(resource_ns.clone());
        let shard = shard.unwrap();
        //println!("Shard id: {}", shard.id);
        if shard.id == self.id {
            return self.handle_command(client_id, command).await;
        }

        let message = ShardMessage::Command(command);
        shard.send_request(client_id, message).await
    }

    pub fn insert_shart_table_record(
        &self,
        resource_ns: IggyResourceNamespace,
        shard_info: ShardInfo,
    ) {
        self.shards_table
            .borrow_mut()
            .insert(resource_ns, shard_info);
    }

    fn find_shard(&self, resource_ns: IggyResourceNamespace) -> Option<&Shard> {
        let shards_table = self.shards_table.borrow();
        shards_table.get(&resource_ns).map(|shard_info| {
            self.shards
                .iter()
                .find(|shard| shard.id == shard_info.id)
                .expect("Shard not found in the shards array.")
        })
    }

    pub async fn handle_shard_message(
        &self,
        client_id: u32,
        message: ShardMessage,
    ) -> Option<ShardResponse> {
        match message {
            ShardMessage::Command(cmd) => match self.handle_command(client_id, cmd).await {
                Ok(response) => Some(response),
                Err(err) => Some(ShardResponse::ErrorResponse(err)),
            },
            ShardMessage::Event(event) => self.handle_event(client_id, event).await.map_or_else(
                |e| {
                    panic!(
                        "Failed to handle event on shard with id: {}, error: {}",
                        self.id, e
                    );
                },
                |_| None,
            ),
        }
    }

    pub async fn clean_cache(&self, size_to_clean: u64) {
        for stream in self.streams.borrow().values() {
            for topic in stream.get_topics() {
                for mut partition in topic.get_partitions().into_iter() {
                    monoio::spawn(async move {
                        let memory_tracker = CacheMemoryTracker::get_instance().unwrap();
                        let cache = partition.cache.as_mut().unwrap();
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
