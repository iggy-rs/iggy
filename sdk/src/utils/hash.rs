use std::hash::Hasher as _;

use hash32::{Hasher, Murmur3Hasher};

use crate::identifier::Identifier;

pub fn hash_resource_namespace(
    stream_id: &Identifier,
    topic_id: &Identifier,
    partition_id: u32,
) -> u32 {
    let mut hasher = Murmur3Hasher::default();
    hasher.write(&stream_id.value);
    hasher.write(&topic_id.value);
    hasher.write_u32(partition_id);
    hasher.finish32()
}
