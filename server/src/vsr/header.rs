use super::{Operation, ProtocolVersion};
use crate::versioning::SemanticVersion;

pub trait IggyHeader {}

#[derive(Default, Debug)]
pub struct Request {
    pub checksum: u128,
    pub checksum_body: u128,
    // Reserved for further usage, for encryption.
    pub nonce_reserved: u128,
    pub cluster: u128,
    pub size: u32,
    // Reserved for further usage.
    pub epoch: u32,
    pub view_number: u32,
    pub version: SemanticVersion,
    // Version of VSR protocol.
    pub protocol_version: ProtocolVersion,
    // Currently not used, since our body has that information in it's payload.
    pub command: u32,
    pub replica: u8,
    // Reserved for further usage.
    pub session_id: u64,
    pub timestamp: u64,
    pub request_number: u32,
    pub operation: Operation,
}

//TODO: Create a method for constructing prepare.
#[derive(Default, Debug)]
pub struct Prepare {
    pub checksum: u128,
    pub checksum_body: u128,
    // Reserved for further usage, for encryption.
    pub nonce_reserved: u128,
    pub cluster: u128,
    pub size: u32,
    // Reserved for further usage.
    pub epoch: u32,
    pub view_number: u32,
    pub version: SemanticVersion,
    // Version of VSR protocol.
    pub protocol_version: ProtocolVersion,
    // Currently not used, since our body has that information in it's payload.
    pub command: u32,
    pub replica: u8,
    // Reserved for further usage.
    pub parent: u128,
    pub request_checksum: u128,
    pub checkpoint_id: u128,
    pub session_id: u64,
    pub op_number: u64,
    pub commit_number: u64,
    pub timestamp: u64,
    pub request_number: u32,
    pub operation: Operation,
}

#[derive(Debug)]
pub struct PrepareOk {
    pub checksum: u128,
    pub checksum_body: u128,
    // Reserved for further usage, for encryption.
    pub nonce_reserved: u128,
    pub cluster: u128,
    pub size: u32,
    // Reserved for further usage.
    pub epoch: u32,
    pub view: u32,
    // Version of VSR Protocol
    pub protocol_version: ProtocolVersion,
    // Currently not used, since our body has that information in it's payload.
    pub command: u32,
    pub replica: u8,
    // Reserved for further usage.
    pub parent: u128,
    pub prepare_checksum: u128,
    pub checkpoint_id: u128,
    pub session_id: u64,
    pub op_number: u64,
    pub commit_number: u64,
    pub timestamp: u64,
    pub request_number: u32,
    pub operation: Operation,
}
