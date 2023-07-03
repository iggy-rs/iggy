use crate::clients::client_manager::{Client, ClientManager, Transport};
use crate::config::SystemConfig;
use crate::persister::*;
use crate::storage::{SegmentStorage, SystemStorage};
use crate::streams::stream::Stream;
use futures::future::join_all;
use sdk::error::Error;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{create_dir, read_dir};
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use tracing::{error, info, trace};

pub struct System {
    pub base_path: String,
    pub streams_path: String,
    pub storage: Arc<SystemStorage>,
    streams: HashMap<u32, Stream>,
    config: Arc<SystemConfig>,
    client_manager: Arc<RwLock<ClientManager>>,
}

impl System {
    pub fn create(config: Arc<SystemConfig>) -> System {
        let base_path = config.path.to_string();
        let streams_path = format!("{}/{}", base_path, &config.stream.path);
        let persister: Arc<dyn Persister> = match config.stream.topic.partition.enforce_sync {
            true => Arc::new(FileWithSyncPersister {}),
            false => Arc::new(FilePersister {}),
        };

        System {
            config,
            base_path,
            streams_path,
            streams: HashMap::new(),
            storage: Arc::new(SystemStorage::new(persister)),
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

        info!("Initializing system...");
        let now = Instant::now();
        self.load_streams().await?;
        info!("Initialized system in {} ms.", now.elapsed().as_millis());
        Ok(())
    }

    async fn load_streams(&mut self) -> Result<(), Error> {
        info!("Loading streams from disk...");
        let mut unloaded_streams = Vec::new();
        let dir_entries = read_dir(&self.streams_path).await;
        if dir_entries.is_err() {
            return Err(Error::CannotReadStreams);
        }

        let mut dir_entries = dir_entries.unwrap();
        while let Some(dir_entry) = dir_entries.next_entry().await.unwrap_or(None) {
            let name = dir_entry.file_name().into_string().unwrap();
            let stream_id = name.parse::<u32>();
            if stream_id.is_err() {
                error!("Invalid stream ID file with name: '{}'.", name);
                continue;
            }

            let stream_id = stream_id.unwrap();
            let stream = Stream::empty(
                stream_id,
                &self.streams_path,
                self.config.stream.clone(),
                self.storage.clone(),
            );
            unloaded_streams.push(stream);
        }

        let loaded_streams = Arc::new(Mutex::new(Vec::new()));
        let mut load_streams = Vec::new();
        for mut stream in unloaded_streams {
            let loaded_streams = loaded_streams.clone();
            let load_stream = tokio::spawn(async move {
                if stream.load().await.is_err() {
                    error!("Failed to load stream with ID: {}.", stream.id);
                    return;
                }

                loaded_streams.lock().await.push(stream);
            });
            load_streams.push(load_stream);
        }

        join_all(load_streams).await;
        for stream in loaded_streams.lock().await.drain(..) {
            self.streams.insert(stream.id, stream);
        }

        info!("Loaded {} stream(s) from disk.", self.streams.len());
        Ok(())
    }

    pub fn get_streams(&self) -> Vec<&Stream> {
        self.streams.values().collect()
    }

    pub fn get_stream(&self, id: u32) -> Result<&Stream, Error> {
        let stream = self.streams.get(&id);
        if stream.is_none() {
            return Err(Error::StreamNotFound(id));
        }

        Ok(stream.unwrap())
    }

    pub fn get_stream_mut(&mut self, id: u32) -> Result<&mut Stream, Error> {
        let stream = self.streams.get_mut(&id);
        if stream.is_none() {
            return Err(Error::StreamNotFound(id));
        }

        Ok(stream.unwrap())
    }

    pub async fn create_stream(&mut self, id: u32, name: &str) -> Result<(), Error> {
        if self.streams.contains_key(&id) {
            return Err(Error::StreamAlreadyExists(id));
        }

        let stream = Stream::create(
            id,
            name,
            &self.streams_path,
            self.config.stream.clone(),
            self.storage.clone(),
        );
        stream.persist().await?;
        self.streams.insert(stream.id, stream);
        info!("Created stream with ID: {}, name: '{}'.", id, name);
        Ok(())
    }

    pub async fn delete_stream(&mut self, id: u32) -> Result<(), Error> {
        let stream = self.get_stream_mut(id)?;
        stream.delete().await?;
        self.streams.remove(&id);
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

    pub fn create_consumer_group(
        &mut self,
        stream_id: u32,
        topic_id: u32,
        group_id: u32,
    ) -> Result<(), Error> {
        self.get_stream_mut(stream_id)?
            .get_topic_mut(topic_id)?
            .create_consumer_group(group_id)?;
        Ok(())
    }

    pub async fn delete_consumer_group(
        &mut self,
        stream_id: u32,
        topic_id: u32,
        group_id: u32,
    ) -> Result<(), Error> {
        let consumer_group = self
            .get_stream_mut(stream_id)?
            .get_topic_mut(topic_id)?
            .delete_consumer_group(group_id)?;

        let client_manager = self.client_manager.read().await;
        let consumer_group = consumer_group.read().await;
        for member in consumer_group.get_members() {
            let member = member.read().await;
            client_manager
                .leave_consumer_group(member.id, stream_id, topic_id, group_id)
                .await?;
        }

        Ok(())
    }

    pub async fn join_consumer_group(
        &self,
        client_id: u32,
        stream_id: u32,
        topic_id: u32,
        group_id: u32,
    ) -> Result<(), Error> {
        self.get_stream(stream_id)?
            .get_topic(topic_id)?
            .join_consumer_group(group_id, client_id)
            .await?;
        let client_manager = self.client_manager.read().await;
        client_manager
            .join_consumer_group(client_id, stream_id, topic_id, group_id)
            .await?;
        Ok(())
    }

    pub async fn leave_consumer_group(
        &self,
        client_id: u32,
        stream_id: u32,
        topic_id: u32,
        group_id: u32,
    ) -> Result<(), Error> {
        self.get_stream(stream_id)?
            .get_topic(topic_id)?
            .leave_consumer_group(group_id, client_id)
            .await?;
        let client_manager = self.client_manager.read().await;
        client_manager
            .leave_consumer_group(client_id, stream_id, topic_id, group_id)
            .await?;
        Ok(())
    }

    pub async fn add_client(&self, address: &SocketAddr, transport: Transport) -> u32 {
        let mut client_manager = self.client_manager.write().await;
        let client_id = client_manager.add_client(address, transport);
        info!("Added {transport} client with ID: {client_id} for address: {address}");
        client_id
    }

    pub async fn delete_client(&self, address: &SocketAddr) {
        let mut client_manager = self.client_manager.write().await;
        let client = client_manager.delete_client(address);
        if client.is_none() {
            return;
        }

        let client = client.unwrap();
        let client = client.read().await;
        info!(
            "Deleted {} client with ID: {} for address: {}",
            client.transport, client.id, client.address
        );

        for consumer_group in client.consumer_groups.iter() {
            if let Err(error) = self
                .leave_consumer_group(
                    client.id,
                    consumer_group.stream_id,
                    consumer_group.topic_id,
                    consumer_group.group_id,
                )
                .await
            {
                error!(
                    "Failed to leave consumer group with ID: {} by client with ID: {}. Error: {}",
                    consumer_group.group_id, client.id, error
                );
            }
        }
    }

    pub async fn get_clients(&self) -> Vec<Arc<RwLock<Client>>> {
        let client_manager = self.client_manager.read().await;
        client_manager.get_clients()
    }
}
