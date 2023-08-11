use crate::error::Error;
use crate::identifier::Identifier;
use crate::messages::send_messages::{Message, Partitioning};
use std::fmt::Debug;

pub trait Partitioner: Send + Sync + Debug {
    fn calculate_partition_id(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &[Message],
    ) -> Result<u32, Error>;
}
