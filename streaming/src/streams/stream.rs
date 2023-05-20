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
}

impl Stream {
    pub fn empty(id: u32, streams_path: &str, config: Arc<StreamConfig>) -> Self {
        Stream::create(id, "", streams_path, config)
    }

    pub fn create(id: u32, name: &str, streams_path: &str, config: Arc<StreamConfig>) -> Self {
        let path = Self::get_path(id, streams_path);
        let info_path = Self::get_info_path(&path);
        let topics_path = Self::get_topics_path(&path, &config.topic.path);

        Stream {
            id,
            name: name.to_string(),
            path,
            topics_path,
            info_path,
            config,
            topics: HashMap::new(),
        }
    }
    
    fn get_path(id: u32, streams_path: &str) -> String {
        format!("{}/{}", streams_path, id)
    }
    
    fn get_info_path(path: &str) -> String {
        format!("{}/{}", path, STREAM_INFO)
    }
    
    fn get_topics_path(path: &str, topics_path: &str) -> String {
        format!("{}/{}", path, topics_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_created_given_valid_parameters() {
        let id = 1;
        let name = "test";
        let streams_path = "/streams";
        let config = Arc::new(StreamConfig::default());
        let path = Stream::get_path(id, streams_path);
        let info_path = Stream::get_info_path(&path);
        let topics_path = Stream::get_topics_path(&path, &config.topic.path);
        
        let stream = Stream::create(id, name, streams_path, config);
        
        assert_eq!(stream.id, id);
        assert_eq!(stream.name, name);
        assert_eq!(stream.path, path);
        assert_eq!(stream.info_path, info_path);
        assert_eq!(stream.topics_path, topics_path);
        assert!(stream.topics.is_empty());
    }
}
