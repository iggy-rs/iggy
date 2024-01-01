pub mod create_topic;
pub mod delete_topic;
pub mod get_topic;
pub mod get_topics;
pub mod purge_topic;
pub mod update_topic;

const MAX_NAME_LENGTH: usize = 255;
const MAX_PARTITIONS_COUNT: u32 = 1000;
