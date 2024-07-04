use crate::identifier::Identifier;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct IggyResourceNamespace {
    pub(crate) stream_id: Identifier,
    pub(crate) topic_id: Identifier,
    pub(crate) partition_id: u32,
}

impl IggyResourceNamespace {
    pub fn new(stream_id: Identifier, topic_id: Identifier, partition_id: u32) -> Self {
        Self {
            stream_id,
            topic_id,
            partition_id,
        }
    }
}
