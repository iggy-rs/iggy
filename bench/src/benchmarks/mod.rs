pub mod benchmark;
pub mod consumer_benchmark;
pub mod consumer_group_benchmark;
pub mod producer_and_consumer_benchmark;
pub mod producer_and_consumer_group_benchmark;
pub mod producer_benchmark;
pub mod producing_consumer_benchmark;

pub const CONSUMER_GROUP_BASE_ID: u32 = 0;
pub const CONSUMER_GROUP_NAME_PREFIX: &str = "cg";
