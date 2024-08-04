use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::messages::send_messages::Message;
use std::fmt::Debug;

/// The trait represent the logic responsible for calculating the partition ID and is used by the `IggyClient`.
/// This might be especially useful when the partition ID is not constant and might be calculated based on the stream ID, topic ID and other parameters.
pub trait Partitioner: Send + Sync + Debug {
    fn calculate_partition_id(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        messages: &[Message],
    ) -> Result<u32, IggyError>;
}
