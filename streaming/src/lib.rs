pub mod topic;
pub mod timestamp;
pub mod partition;
pub mod message;
pub mod index;
pub mod serialization;
pub mod stream;
pub mod system;
pub mod stream_error;
mod segment;

const DATA_PATH: &str = "local_data";
const TOPICS_PATH: &str = "topics";
const TOPIC_INFO: &str = "topic.info";

pub fn get_base_path() -> String {
    DATA_PATH.to_string()
}

pub fn get_topics_path() -> String {
    format!("{}/{}", &get_base_path(), TOPICS_PATH)
}