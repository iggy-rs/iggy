use std::path::Path;
use tracing::{error, info};
use crate::{get_base_path, get_topics_path};
use crate::stream::Stream;
use crate::stream_error::StreamError;
use crate::topic::Topic;

pub async fn init() -> Result<Stream, StreamError> {
    let base_path = &get_base_path();
    if !Path::new(base_path).exists() && std::fs::create_dir(base_path).is_err() {
        return Err(StreamError::CannotCreateBaseDirectory);
    }

    let topics_path = &get_topics_path();
    if !Path::new(topics_path).exists() && std::fs::create_dir(topics_path).is_err() {
        return Err(StreamError::CannotCreateTopicsDirectory);
    }

    let mut stream = Stream::create();
    load_topics_from_disk(&mut stream).await;
    Ok(stream)
}

async fn load_topics_from_disk(stream: &mut Stream) {
    info!("Loading topics from disk...");
    let topics_path = &get_topics_path();
    let topics = std::fs::read_dir(topics_path).unwrap();
    for topic in topics {
        info!("Trying to load topic from disk...");
        let topic = topic.unwrap();
        let topic_id = topic.file_name().into_string().unwrap().parse::<u32>().unwrap();
        info!("Loading topic with ID: {} from disk...", topic_id);
        let topic = Topic::load_from_disk(topic_id).await;
        if topic.is_err() {
            error!("{}", topic.err().unwrap());
            continue;
        }

        let topic = topic.unwrap();
        info!("Loaded topic with ID: {} from disk.", topic_id);
        stream.topics.insert(topic_id, topic);
    }

    info!("Loaded {} topic(s) from disk.", stream.topics.len());
}