use crate::config::SystemConfig;
use crate::streams::stream::Stream;
use shared::error::Error;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tracing::{error, info, trace};

pub struct System {
    pub base_path: String,
    pub streams_path: String,
    streams: HashMap<u32, Stream>,
    config: Arc<SystemConfig>,
}

impl System {
    pub fn create(config: Arc<SystemConfig>) -> System {
        let base_path = config.path.to_string();
        let streams_path = format!("{}/{}", base_path, &config.stream.path);

        System {
            config,
            base_path,
            streams_path,
            streams: HashMap::new(),
        }
    }

    pub async fn init(&mut self) -> Result<(), Error> {
        if !Path::new(&self.base_path).exists() && std::fs::create_dir(&self.base_path).is_err() {
            return Err(Error::CannotCreateBaseDirectory);
        }

        if !Path::new(&self.streams_path).exists()
            && std::fs::create_dir(&self.streams_path).is_err()
        {
            return Err(Error::CannotCreateStreamsDirectory);
        }

        self.load_streams().await?;

        Ok(())
    }

    async fn load_streams(&mut self) -> Result<(), Error> {
        info!("Loading streams from disk...");
        let streams = std::fs::read_dir(&self.streams_path).unwrap();
        for stream in streams {
            info!("Trying to load stream from disk...");
            let name = stream.unwrap().file_name().into_string().unwrap();
            let stream_id = name.parse::<u32>();
            if stream_id.is_err() {
                error!("Invalid stream ID file with name: '{}'.", name);
                continue;
            }

            let stream_id = stream_id.unwrap();
            let mut stream =
                Stream::empty(stream_id, &self.streams_path, self.config.stream.clone());
            stream.load().await?;
            self.streams.insert(stream_id, stream);
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

        let stream = Stream::create(id, name, &self.streams_path, self.config.stream.clone());
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

    pub async fn shutdown(&mut self) -> Result<(), Error> {
        self.persist_messages(true).await?;
        Ok(())
    }

    pub async fn persist_messages(&self, enforce_sync: bool) -> Result<(), Error> {
        trace!("Saving buffered messages on disk...");
        for stream in self.streams.values() {
            stream.persist_messages(enforce_sync).await?;
        }

        Ok(())
    }
}
