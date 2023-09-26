use crate::configs::system::SystemConfig;
use crate::streaming::clients::client_manager::ClientManager;
use crate::streaming::diagnostics::metrics::Metrics;
use crate::streaming::persistence::persister::*;
use crate::streaming::session::Session;
use crate::streaming::storage::{SegmentStorage, SystemStorage};
use crate::streaming::streams::stream::Stream;
use crate::streaming::users::permissioner::Permissioner;
use iggy::error::Error;
use iggy::utils::crypto::{Aes256GcmEncryptor, Encryptor};
use sled::Db;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::create_dir;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::{info, trace};

#[derive(Debug)]
pub struct System {
    pub permissioner: Permissioner,
    pub(crate) storage: Arc<SystemStorage>,
    pub(crate) streams: HashMap<u32, Stream>,
    pub(crate) streams_ids: HashMap<String, u32>,
    pub(crate) config: Arc<SystemConfig>,
    pub(crate) client_manager: Arc<RwLock<ClientManager>>,
    pub(crate) encryptor: Option<Box<dyn Encryptor>>,
    pub(crate) metrics: Metrics,
}

impl System {
    pub fn new(config: Arc<SystemConfig>, db: Option<Arc<Db>>) -> System {
        let db = match db {
            Some(db) => db,
            None => {
                let db = sled::open(config.get_database_path());
                if db.is_err() {
                    panic!("Cannot open database at: {}", config.get_database_path());
                }
                Arc::new(db.unwrap())
            }
        };
        let persister: Arc<dyn Persister> = match config.partition.enforce_fsync {
            true => Arc::new(FileWithSyncPersister {}),
            false => Arc::new(FilePersister {}),
        };
        Self::create(config, SystemStorage::new(db, persister))
    }

    pub fn create(config: Arc<SystemConfig>, storage: SystemStorage) -> System {
        info!(
            "Server-side encryption is {}.",
            Self::map_toggle_str(config.encryption.enabled)
        );
        System {
            encryptor: match config.encryption.enabled {
                true => Some(Box::new(
                    Aes256GcmEncryptor::from_base64_key(&config.encryption.key).unwrap(),
                )),
                false => None,
            },
            config,
            streams: HashMap::new(),
            streams_ids: HashMap::new(),
            storage: Arc::new(storage),
            client_manager: Arc::new(RwLock::new(ClientManager::default())),
            permissioner: Permissioner::default(),
            metrics: Metrics::init(),
        }
    }

    pub async fn init(&mut self) -> Result<(), Error> {
        if !Path::new(&self.config.get_system_path()).exists()
            && create_dir(&self.config.get_system_path()).await.is_err()
        {
            return Err(Error::CannotCreateBaseDirectory);
        }
        if !Path::new(&self.config.get_streams_path()).exists()
            && create_dir(&self.config.get_streams_path()).await.is_err()
        {
            return Err(Error::CannotCreateStreamsDirectory);
        }

        info!(
            "Initializing system, data will be stored at: {}",
            self.config.get_system_path()
        );
        let now = Instant::now();
        self.load_version().await?;
        self.load_users().await?;
        self.load_streams().await?;
        info!("Initialized system in {} ms.", now.elapsed().as_millis());
        Ok(())
    }

    pub async fn shutdown(&mut self, storage: Arc<dyn SegmentStorage>) -> Result<(), Error> {
        self.persist_messages(storage.clone()).await?;
        Ok(())
    }

    pub async fn persist_messages(&self, storage: Arc<dyn SegmentStorage>) -> Result<(), Error> {
        trace!("Saving buffered messages on disk...");
        for stream in self.streams.values() {
            stream.persist_messages(storage.clone()).await?;
        }

        Ok(())
    }

    pub fn ensure_authenticated(&self, session: &Session) -> Result<(), Error> {
        match session.is_authenticated() {
            true => Ok(()),
            false => Err(Error::Unauthenticated),
        }
    }

    fn map_toggle_str<'a>(enabled: bool) -> &'a str {
        match enabled {
            true => "enabled",
            false => "disabled",
        }
    }
}
