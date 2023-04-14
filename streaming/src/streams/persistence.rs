use crate::error::Error;
use crate::streams::stream::Stream;
use crate::topics::topic::Topic;
use tracing::info;

impl Stream {
    pub async fn load(&mut self) -> Result<(), Error> {
        info!("Loading topics from disk...");
        let topics = std::fs::read_dir(&self.topics_path).unwrap();
        for topic in topics {
            info!("Trying to load topic from disk...");
            let topic = topic.unwrap();
            let topic_id = topic
                .file_name()
                .into_string()
                .unwrap()
                .parse::<u32>()
                .unwrap();
            let mut topic = Topic::empty(topic_id, &self.topics_path, self.config.topic.clone());
            topic.load().await?;
            self.topics.insert(topic_id, topic);
        }

        info!("Loaded {} topic(s) from disk.", self.topics.len());

        Ok(())
    }
}
