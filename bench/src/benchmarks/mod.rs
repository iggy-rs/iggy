pub mod benchmark;
pub mod consumer_group_benchmark;
pub mod poll_benchmark;
pub mod send_and_poll_benchmark;
pub mod send_benchmark;

pub const CONSUMER_GROUP_BASE_ID: u32 = 0;
pub const CONSUMER_GROUP_NAME_PREFIX: &str = "cg";
