pub mod config;
pub mod index;
pub mod message;
pub mod partition;
pub mod segment;
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
