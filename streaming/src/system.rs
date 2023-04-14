use crate::config::StreamConfig;
use crate::error::Error;
use crate::streams::stream::Stream;
use std::path::Path;
use tracing::info;

pub struct System {
    pub stream: Stream,
}

impl System {
    pub async fn init(config: StreamConfig) -> Result<System, Error> {
        let base_path = &config.path;
        let topics_path = &config.topic.path;
        if !Path::new(base_path).exists() && std::fs::create_dir(base_path).is_err() {
            return Err(Error::CannotCreateBaseDirectory);
        }

        let topics_path = &format!("{}/{}", base_path, topics_path);
        if !Path::new(topics_path).exists() && std::fs::create_dir(topics_path).is_err() {
            return Err(Error::CannotCreateTopicsDirectory);
        }

        let mut stream = Stream::create(config);
        stream.load().await?;
        let system = System { stream };
        Ok(system)
    }

    pub async fn shutdown(&mut self) -> Result<(), Error> {
        self.persist_messages().await?;
        Ok(())
    }

    pub async fn persist_messages(&mut self) -> Result<(), Error> {
        info!("Saving existing messages on disk...");
        for topic in self.stream.get_topics_mut() {
            topic.persist_messages().await?;
        }
        Ok(())
    }
}
