use crate::config::StreamConfig;
use crate::stream::Stream;
use crate::stream_error::StreamError;
use crate::{get_base_path, get_topics_path};
use std::path::Path;
use tracing::info;

pub struct System {
    pub stream: Stream,
}

impl System {
    pub async fn init(config: StreamConfig) -> Result<System, StreamError> {
        info!("Initializing Iggy server...");
        let base_path = &get_base_path();
        if !Path::new(base_path).exists() && std::fs::create_dir(base_path).is_err() {
            return Err(StreamError::CannotCreateBaseDirectory);
        }

        let topics_path = &get_topics_path();
        if !Path::new(topics_path).exists() && std::fs::create_dir(topics_path).is_err() {
            return Err(StreamError::CannotCreateTopicsDirectory);
        }

        let mut stream = Stream::create(config);
        stream.load_topics_from_disk().await;
        let system = System { stream };
        Ok(system)
    }

    pub async fn shutdown(&mut self) -> Result<(), StreamError> {
        self.save_existing_messages().await?;
        Ok(())
    }

    pub async fn save_existing_messages(&mut self) -> Result<(), StreamError> {
        info!("Saving existing messages on disk...");
        for topic in self.stream.get_topics_mut() {
            topic.save_existing_messages().await?;
        }
        Ok(())
    }
}
