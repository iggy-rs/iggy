use crate::clients::client_manager::ClientManager;
use crate::config::SystemConfig;
use crate::persister::*;
use crate::storage::{SegmentStorage, SystemStorage};
use crate::streams::stream::Stream;
use aes_gcm::{Aes256Gcm, KeyInit};
use iggy::error::Error;
use iggy::utils::text;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::create_dir;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::{info, trace};

pub struct System {
    pub base_path: String,
    pub streams_path: String,
    pub storage: Arc<SystemStorage>,
    pub(crate) streams: HashMap<u32, Stream>,
    pub(crate) streams_ids: HashMap<String, u32>,
    pub(crate) config: Arc<SystemConfig>,
    pub(crate) client_manager: Arc<RwLock<ClientManager>>,
    pub(crate) cipher: Option<Aes256Gcm>,
}

impl System {
    pub fn new(config: Arc<SystemConfig>) -> System {
        let persister: Arc<dyn Persister> = match config.stream.topic.partition.enforce_sync {
            true => Arc::new(FileWithSyncPersister {}),
            false => Arc::new(FilePersister {}),
        };
        Self::create(config, SystemStorage::new(persister))
    }

    pub fn create(config: Arc<SystemConfig>, storage: SystemStorage) -> System {
        let base_path = config.path.to_string();
        let streams_path = format!("{}/{}", base_path, &config.stream.path);
        let encryption_key = if config.encryption.enabled {
            Some(text::from_base64_as_bytes(&config.encryption.key).unwrap())
        } else {
            None
        };

        if encryption_key.is_some() {
            if encryption_key.as_ref().unwrap().len() != 32 {
                panic!("Encryption key must be 32 bytes long.");
            }
            info!("Server-side encryption is enabled.");
        }

        System {
            config,
            base_path,
            streams_path,
            streams: HashMap::new(),
            streams_ids: HashMap::new(),
            storage: Arc::new(storage),
            client_manager: Arc::new(RwLock::new(ClientManager::new())),
            cipher: encryption_key.map(|key| Aes256Gcm::new(key.as_slice().into())),
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
