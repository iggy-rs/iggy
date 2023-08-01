use crate::clients::client_manager::{Client, ClientManager, Transport};
use crate::config::SystemConfig;
use crate::persister::*;
use crate::storage::{SegmentStorage, SystemStorage};
use crate::streams::stream::Stream;
use crate::utils::text;
use futures::future::join_all;
use iggy::error::Error;
use iggy::models::stats::Stats;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use sysinfo::{PidExt, ProcessExt, SystemExt};
use tokio::fs::{create_dir, read_dir};
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use tracing::{error, info, trace};

const PROCESS_NAME: &str = "server";

pub struct System {
    pub base_path: String,
    pub streams_path: String,
    pub storage: Arc<SystemStorage>,
    streams: HashMap<u32, Stream>,
    streams_ids: HashMap<String, u32>,
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
            streams_ids: HashMap::new(),
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

        info!(
            "Initializing system, data will be stored at: {}",
            self.base_path
        );
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
            if self.streams.contains_key(&stream.id) {
                error!("Stream with ID: '{}' already exists.", &stream.id);
                continue;
            }

            if self.streams_ids.contains_key(&stream.name) {
                error!("Stream with name: '{}' already exists.", &stream.name);
                continue;
            }

            self.streams_ids.insert(stream.name.clone(), stream.id);
            self.streams.insert(stream.id, stream);
        }

        info!("Loaded {} stream(s) from disk.", self.streams.len());
        Ok(())
    }

    pub fn get_streams(&self) -> Vec<&Stream> {
        self.streams.values().collect()
    }

    pub fn get_stream_by_id(&self, id: u32) -> Result<&Stream, Error> {
        let stream = self.streams.get(&id);
        if stream.is_none() {
            return Err(Error::StreamIdNotFound(id));
        }

        Ok(stream.unwrap())
    }

    pub fn get_stream_by_name(&self, name: &str) -> Result<&Stream, Error> {
        let stream_id = self.streams_ids.get(name);
        if stream_id.is_none() {
            return Err(Error::StreamNameNotFound(name.to_string()));
        }

        self.get_stream_by_id(*stream_id.unwrap())
    }

    pub fn get_stream_by_id_mut(&mut self, id: u32) -> Result<&mut Stream, Error> {
        let stream = self.streams.get_mut(&id);
        if stream.is_none() {
            return Err(Error::StreamIdNotFound(id));
        }

        Ok(stream.unwrap())
    }

    pub fn get_stream_by_name_mut(&mut self, name: &str) -> Result<&mut Stream, Error> {
        let stream_id = self.streams_ids.get_mut(name);
        if stream_id.is_none() {
            return Err(Error::StreamNameNotFound(name.to_string()));
        }

        Ok(self.streams.get_mut(stream_id.unwrap()).unwrap())
    }

    pub async fn create_stream(&mut self, id: u32, name: &str) -> Result<(), Error> {
        if self.streams.contains_key(&id) {
            return Err(Error::StreamIdAlreadyExists(id));
        }

        let name = text::to_lowercase_non_whitespace(name);
        if self.streams_ids.contains_key(&name) {
            return Err(Error::StreamNameAlreadyExists(name.to_string()));
        }

        let stream = Stream::create(
            id,
            &name,
            &self.streams_path,
            self.config.stream.clone(),
            self.storage.clone(),
        );
        stream.persist().await?;
        info!("Created stream with ID: {}, name: '{}'.", id, name);
        self.streams_ids.insert(name, stream.id);
        self.streams.insert(stream.id, stream);
        Ok(())
    }

    pub async fn delete_stream(&mut self, id: u32) -> Result<(), Error> {
        let stream = self.get_stream_by_id_mut(id)?;
        let name = stream.name.clone();
        stream.delete().await?;
        self.streams_ids.remove(&name);
        self.streams.remove(&id);
        let client_manager = self.client_manager.read().await;
        client_manager.delete_consumer_groups_for_stream(id).await;
        Ok(())
    }

    pub async fn delete_topic(&mut self, stream_id: u32, topic_id: u32) -> Result<(), Error> {
        self.get_stream_by_id_mut(stream_id)?
            .delete_topic(topic_id)
            .await?;
        let client_manager = self.client_manager.read().await;
        client_manager
            .delete_consumer_groups_for_topic(stream_id, topic_id)
            .await;
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

    pub async fn create_consumer_group(
        &mut self,
        stream_id: u32,
        topic_id: u32,
        consumer_group_id: u32,
    ) -> Result<(), Error> {
        self.get_stream_by_id_mut(stream_id)?
            .get_topic_by_id_mut(topic_id)?
            .create_consumer_group(consumer_group_id)
            .await?;
        Ok(())
    }

    pub async fn delete_consumer_group(
        &mut self,
        stream_id: u32,
        topic_id: u32,
        consumer_group_id: u32,
    ) -> Result<(), Error> {
        let consumer_group = self
            .get_stream_by_id_mut(stream_id)?
            .get_topic_by_id_mut(topic_id)?
            .delete_consumer_group(consumer_group_id)
            .await?;

        let client_manager = self.client_manager.read().await;
        let consumer_group = consumer_group.read().await;
        for member in consumer_group.get_members() {
            let member = member.read().await;
            client_manager
                .leave_consumer_group(member.id, stream_id, topic_id, consumer_group_id)
                .await?;
        }

        Ok(())
    }

    pub async fn join_consumer_group(
        &self,
        client_id: u32,
        stream_id: u32,
        topic_id: u32,
        consumer_group_id: u32,
    ) -> Result<(), Error> {
        self.get_stream_by_id(stream_id)?
            .get_topic_by_id(topic_id)?
            .join_consumer_group(consumer_group_id, client_id)
            .await?;
        let client_manager = self.client_manager.read().await;
        client_manager
            .join_consumer_group(client_id, stream_id, topic_id, consumer_group_id)
            .await?;
        Ok(())
    }

    pub async fn leave_consumer_group(
        &self,
        client_id: u32,
        stream_id: u32,
        topic_id: u32,
        consumer_group_id: u32,
    ) -> Result<(), Error> {
        self.get_stream_by_id(stream_id)?
            .get_topic_by_id(topic_id)?
            .leave_consumer_group(consumer_group_id, client_id)
            .await?;
        let client_manager = self.client_manager.read().await;
        client_manager
            .leave_consumer_group(client_id, stream_id, topic_id, consumer_group_id)
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
        let consumer_groups: Vec<(u32, u32, u32)>;
        let client_id;

        {
            let client_manager = self.client_manager.read().await;
            let client = client_manager.get_client_by_address(address);
            if client.is_err() {
                return;
            }

            let client = client.unwrap();
            let client = client.read().await;
            client_id = client.id;

            consumer_groups = client
                .consumer_groups
                .iter()
                .map(|c| (c.stream_id, c.topic_id, c.consumer_group_id))
                .collect();
        }

        for (stream_id, topic_id, consumer_group_id) in consumer_groups.iter() {
            if let Err(error) = self
                .leave_consumer_group(client_id, *stream_id, *topic_id, *consumer_group_id)
                .await
            {
                error!(
                    "Failed to leave consumer group with ID: {} by client with ID: {}. Error: {}",
                    consumer_group_id, client_id, error
                );
            }
        }

        {
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
        }
    }

    pub async fn get_client(&self, client_id: u32) -> Result<Arc<RwLock<Client>>, Error> {
        let client_manager = self.client_manager.read().await;
        client_manager.get_client_by_id(client_id)
    }

    pub async fn get_clients(&self) -> Vec<Arc<RwLock<Client>>> {
        let client_manager = self.client_manager.read().await;
        client_manager.get_clients()
    }

    pub async fn get_stats(&self) -> Stats {
        let mut sys = sysinfo::System::new_all();
        sys.refresh_system();
        sys.refresh_processes();

        let mut stats = Stats {
            process_id: 0,
            cpu_usage: 0.0,
            memory_usage: 0,
            total_memory: 0,
            available_memory: 0,
            run_time: 0,
            start_time: 0,
            streams_count: self.streams.len() as u32,
            topics_count: self
                .streams
                .values()
                .map(|s| s.topics.len() as u32)
                .sum::<u32>(),
            partitions_count: self
                .streams
                .values()
                .map(|s| {
                    s.topics
                        .values()
                        .map(|t| t.partitions.len() as u32)
                        .sum::<u32>()
                })
                .sum::<u32>(),
            segments_count: 0,
            messages_count: 0,
            clients_count: self.client_manager.read().await.get_clients().len() as u32,
            consumer_groups_count: self
                .streams
                .values()
                .map(|s| {
                    s.topics
                        .values()
                        .map(|t| t.consumer_groups.len() as u32)
                        .sum::<u32>()
                })
                .sum::<u32>(),
            read_bytes: 0,
            written_bytes: 0,
            messages_size_bytes: 0,
            hostname: sys.host_name().unwrap_or("unknown_hostname".to_string()),
            os_name: sys.name().unwrap_or("unknown_os_name".to_string()),
            os_version: sys
                .long_os_version()
                .unwrap_or("unknown_os_version".to_string()),
            kernel_version: sys
                .kernel_version()
                .unwrap_or("unknown_kernel_version".to_string()),
        };

        for (pid, process) in sys.processes() {
            if process.name() != PROCESS_NAME {
                continue;
            }

            stats.process_id = pid.as_u32();
            stats.cpu_usage = process.cpu_usage();
            stats.memory_usage = process.memory();
            stats.total_memory = sys.total_memory();
            stats.available_memory = sys.available_memory();
            stats.run_time = process.run_time();
            stats.start_time = process.start_time();
            let disk_usage = process.disk_usage();
            stats.read_bytes = disk_usage.total_read_bytes;
            stats.written_bytes = disk_usage.total_written_bytes;
            break;
        }

        for stream in self.streams.values() {
            for topic in stream.topics.values() {
                for partition in topic.partitions.values() {
                    let partition = partition.read().await;
                    stats.messages_count += partition.get_messages_count();
                    stats.segments_count += partition.segments.len() as u32;
                    for segment in &partition.segments {
                        stats.messages_size_bytes += segment.current_size_bytes as u64;
                    }
                }
            }
        }

        stats
    }
}
