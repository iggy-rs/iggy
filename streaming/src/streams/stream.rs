use crate::config::StreamConfig;
use crate::topics::topic::Topic;
use std::collections::HashMap;
use std::sync::Arc;

const STREAM_INFO: &str = "stream.info";

#[derive(Debug)]
pub struct Stream {
    pub id: u32,
    pub name: String,
    pub topics: HashMap<u32, Topic>,
    pub(crate) path: String,
    pub(crate) topics_path: String,
    pub(crate) config: Arc<StreamConfig>,
    pub(crate) info_path: String,
    pub(crate) max_messages_in_batch: usize,
}

impl Stream {
    pub fn empty(id: u32, streams_path: &str, config: Arc<StreamConfig>) -> Self {
        Stream::create(id, "", streams_path, config)
    }

    pub fn create(id: u32, name: &str, streams_path: &str, config: Arc<StreamConfig>) -> Self {
        let path = format!("{}/{}", streams_path, id);
        let info_path = format!("{}/{}", path, STREAM_INFO);
        let topics_path = format!("{}/{}", path, &config.topic.path);
        let max_messages_in_batch = config.topic.partition.segment.messages_buffer as usize;

        Stream {
            id,
            name: name.to_string(),
            path,
            topics_path,
            info_path,
            config,
            topics: HashMap::new(),
            max_messages_in_batch,
        }
    }
}
