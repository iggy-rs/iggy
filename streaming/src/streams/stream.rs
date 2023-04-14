use crate::config::StreamConfig;
use crate::topics::topic::Topic;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct Stream {
    pub id: u32,
    pub name: String,
    pub topics: HashMap<u32, Topic>,
    pub(crate) config: Arc<StreamConfig>,
    pub(crate) topics_path: String,
}

//TODO: Allow multiple streams to be created e.g. dev, test, prod etc.
impl Stream {
    pub fn create(config: StreamConfig) -> Self {
        let topics_path = format!("{}/{}", &config.path, &config.topic.path);
        Stream {
            id: 1,
            name: "default".to_string(),
            config: Arc::new(config),
            topics: HashMap::new(),
            topics_path,
        }
    }
}
