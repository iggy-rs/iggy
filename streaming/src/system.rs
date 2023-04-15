use crate::config::SystemConfig;
use crate::error::Error;
use crate::streams::stream::Stream;
use std::collections::HashMap;
use std::path::Path;
use tracing::info;

pub struct System {
    pub streams: HashMap<u32, Stream>,
    pub(crate) config: SystemConfig,
    streams_path: String,
}

impl System {
    pub async fn init(config: SystemConfig) -> Result<System, Error> {
        let base_path = &config.path;
        if !Path::new(base_path).exists() && std::fs::create_dir(base_path).is_err() {
            return Err(Error::CannotCreateBaseDirectory);
        }

        let streams_path = format!("{}/{}", base_path, &config.stream.path);
        if !Path::new(&streams_path).exists() && std::fs::create_dir(&streams_path).is_err() {
            return Err(Error::CannotCreateStreamsDirectory);
        }

        let mut system = System {
            config,
            streams_path,
            streams: HashMap::new(),
        };

        system.load_streams().await?;

        Ok(system)
    }

    async fn load_streams(&mut self) -> Result<(), Error> {
        info!("Loading streams from disk...");
        let streams = std::fs::read_dir(&self.streams_path).unwrap();
        for stream in streams {
            info!("Trying to load stream from disk...");
            let stream_id = stream
                .unwrap()
                .file_name()
                .into_string()
                .unwrap()
                .parse::<u32>()
                .unwrap();
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
        let stream = Stream::create(id, name, &self.streams_path, self.config.stream.clone());
        stream.persist().await?;
        self.streams.insert(stream.id, stream);
        Ok(())
    }

    pub async fn delete_stream(&mut self, id: u32) -> Result<(), Error> {
        let stream = self.get_stream_mut(id)?;
        stream.delete().await?;
        self.streams.remove(&id);
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), Error> {
        self.persist_messages().await?;
        Ok(())
    }

    pub async fn persist_messages(&mut self) -> Result<(), Error> {
        info!("Saving buffered messages on disk...");
        for stream in self.streams.values_mut() {
            stream.persist_messages().await?;
        }

        Ok(())
    }
}
