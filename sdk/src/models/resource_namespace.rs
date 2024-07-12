use crate::identifier::Identifier;
use hash32::{Hasher, Murmur3Hasher};
use std::hash::Hasher as _;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct IggyResourceNamespace {
    pub(crate) stream_id: u32,
    pub(crate) topic_id: u32,
    pub(crate) partition_id: u32,
}

impl IggyResourceNamespace {
    pub fn new(stream_id: u32, topic_id: u32, partition_id: u32) -> Self {
        Self {
            stream_id,
            topic_id,
            partition_id,
        }
    }

    pub fn generate_hash(&self) -> u32 {
        hash_resource_namespace(self.stream_id, self.topic_id, self.partition_id)
    }
}

fn hash_resource_namespace(stream_id: u32, topic_id: u32, partition_id: u32) -> u32 {
    let mut hasher = Murmur3Hasher::default();
    hasher.write_u32(stream_id);
    hasher.write_u32(topic_id);
    hasher.write_u32(partition_id);
    hasher.finish32()
}
