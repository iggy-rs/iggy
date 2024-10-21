use crate::versioning::SemanticVersion;
use super::{Operation, ProtocolVersion};

pub enum Header {
    // TODO: Add Request header, for now let's just use the `Command` enum
    // and ignore the request_number and all of the client side data required by VSR.
    Prepare(Prepare),
    PrepareOk(PrepareOk),
}

#[derive(Debug, Clone)]
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
    pub release: SemanticVersion,
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

#[derive(Debug, Clone)]
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
    pub release: SemanticVersion,
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
