use crate::clients::client_manager::ClientManager;
use crate::config::SystemConfig;
use crate::persister::*;
use crate::storage::{SegmentStorage, SystemStorage};
use crate::streams::stream::Stream;
use iggy::error::Error;
use iggy::utils::crypto::{Aes256GcmEncryptor, Encryptor};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::create_dir;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::{info, trace};

#[derive(Debug)]
pub struct System {
    pub base_path: String,
    pub streams_path: String,
    pub storage: Arc<SystemStorage>,
    pub(crate) streams: HashMap<u32, Stream>,
    pub(crate) streams_ids: HashMap<String, u32>,
    pub(crate) config: Arc<SystemConfig>,
    pub(crate) client_manager: Arc<RwLock<ClientManager>>,
    pub(crate) encryptor: Option<Box<dyn Encryptor>>,
}

impl System {
    pub fn new(config: Arc<SystemConfig>) -> System {
        let persister: Arc<dyn Persister> = match config.partition.enforce_fsync {
            true => Arc::new(FileWithSyncPersister {}),
            false => Arc::new(FilePersister {}),
        };
        Self::create(config, SystemStorage::new(persister))
    }

    pub fn create(config: Arc<SystemConfig>, storage: SystemStorage) -> System {
        if config.encryption.enabled {
            info!("Server-side encryption is enabled.");
        }

        System {
            encryptor: match config.encryption.enabled {
                true => Some(Box::new(
                    Aes256GcmEncryptor::new_from_base64_key(&config.encryption.key).unwrap(),
                )),
                false => None,
            },
            base_path: config.get_system_path(),
            streams_path: config.get_streams_path(),
            config,
            streams: HashMap::new(),
            streams_ids: HashMap::new(),
            storage: Arc::new(storage),
            client_manager: Arc::new(RwLock::new(ClientManager::new())),
        }
    }

    pub async fn init(&mut self) -> Result<(), Error> {
        if !Path::new(&self.base_path).exists() && create_dir(&self.base_path).await.is_err() {
            return Err(Error::CannotCreateBaseDirectory);
        }
        if !Path::new(&self.streams_path).exists() && create_dir(&self.streams_path).await.is_err()
        {
            return Err(Error::CannotCreateStreamsDirectory);
        }

        info!(
            "Initializing system, data will be stored at: {}",
            self.base_path
        );
        let now = Instant::now();
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
}
