use crate::stream_error::StreamError;

pub mod config;
pub mod index;
pub mod message;
pub mod partition;
pub mod segments;
pub mod serialization;
pub mod stream;
pub mod stream_error;
pub mod system;
pub mod timestamp;
pub mod topic;

const DATA_PATH: &str = "local_data";
const TOPICS_PATH: &str = "topics";
const TOPIC_INFO: &str = "topic.info";

pub fn get_base_path() -> String {
    DATA_PATH.to_string()
}

pub fn get_topics_path() -> String {
    format!("{}/{}", &get_base_path(), TOPICS_PATH)
}

pub trait Loadable {
    fn load(&mut self, path: &str) -> Result<(), StreamError>;
}

pub trait Saveable {
    fn save(&self, path: &str) -> Result<(), StreamError>;
}
