use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::command;
use iggy::error::IggyError;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::timestamp::IggyTimestamp;
use std::fmt::{Debug, Display, Formatter};

pub mod file;
pub mod system;

#[async_trait]
pub trait State: Send + Sync + Debug {
    async fn init(&self) -> Result<(), IggyError>;
    async fn load_entries(&self) -> Result<Vec<StateEntry>, IggyError>;
    async fn apply(
        &self,
        code: u32,
        user_id: u32,
        payload: &[u8],
        context: Option<&[u8]>,
    ) -> Result<(), IggyError>;
}

/// State entry in the log
/// - `index` - Commit index of the entry in the log
/// - `term` - Election term for replication
/// - `version` - Server version based on semver as number e.g. 1.234.567 -> 1234567
/// - `flags` - Reserved for future use
/// - `timestamp` - Timestamp when the command was issued
/// - `user_id` - User ID of the user who issued the command
/// - `code` - Command code
/// - `payload` - Payload of the command
/// - `context` - Optional context e.g. used to enrich the payload with additional data
#[derive(Debug)]
pub struct StateEntry {
    pub index: u64,
    pub term: u64,
    pub version: u32,
    pub flags: u64,
    pub timestamp: IggyTimestamp,
    pub user_id: u32,
    pub code: u32,
    pub payload: Bytes,
    pub context: Bytes,
}

impl Display for StateEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StateEntry {{ index: {}, term: {}, version: {}, flags: {}, timestamp: {}, user ID: {}, code: {}, name: {}, size: {} }}",
            self.index,
            self.term,
            self.version,
            self.flags,
            self.timestamp,
            self.user_id,
            self.code,
            command::get_name_from_code(self.code).unwrap_or("invalid_command"),
            IggyByteSize::from(self.payload.len() as u64).as_human_string()
        )
    }
}

impl BytesSerializable for StateEntry {
    fn as_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(
            8 + 8 + 4 + 8 + 8 + 4 + 4 + 4 + self.payload.len() + 4 + self.context.len(),
        );
        bytes.put_u64_le(self.index);
        bytes.put_u64_le(self.term);
        bytes.put_u32_le(self.version);
        bytes.put_u64_le(self.flags);
        bytes.put_u64_le(self.timestamp.to_micros());
        bytes.put_u32_le(self.user_id);
        bytes.put_u32_le(self.code);
        bytes.put_u32_le(self.payload.len() as u32);
        bytes.put_slice(&self.payload);
        bytes.put_u32_le(self.context.len() as u32);
        bytes.put_slice(&self.context);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let index = bytes.slice(0..8).get_u64_le();
        let term = bytes.slice(8..16).get_u64_le();
        let version = bytes.slice(16..20).get_u32_le();
        let flags = bytes.slice(20..28).get_u64_le();
        let timestamp = IggyTimestamp::from(bytes.slice(28..36).get_u64_le());
        let user_id = bytes.slice(36..40).get_u32_le();
        let code = bytes.slice(40..44).get_u32_le();
        let payload_length = bytes.slice(44..48).get_u32_le() as usize;
        let payload = bytes.slice(48..48 + payload_length);
        let context_length = bytes
            .slice(48 + payload_length..52 + payload_length)
            .get_u32_le() as usize;
        let context = bytes.slice(52 + payload_length..52 + payload_length + context_length);
        Ok(StateEntry {
            index,
            term,
            version,
            flags,
            timestamp,
            user_id,
            code,
            payload,
            context,
        })
    }
}
