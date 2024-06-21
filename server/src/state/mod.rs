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
    async fn init(&self) -> Result<Vec<StateEntry>, IggyError>;
    async fn apply(
        &self,
        code: u32,
        user_id: u32,
        payload: &[u8],
        context: Option<&[u8]>,
    ) -> Result<(), IggyError>;
}

#[derive(Debug)]
pub struct StateEntry {
    pub index: u64,
    pub term: u64,
    pub flags: u64,
    pub timestamp: IggyTimestamp,
    pub user_id: u32,
    pub code: u32,
    pub payload: Bytes, // Payload of the command
    pub context: Bytes, // Optional context e.g. used to enrich the payload with additional data
}

impl Display for StateEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StateEntry {{ index: {}, term: {}, flags: {}, timestamp: {}, user ID: {}, code: {}, name: {}, size: {} }}",
            self.index,
            self.term,
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
            8 + 8 + 8 + 4 + 4 + 4 + self.payload.len() + 4 + self.context.len(),
        );
        bytes.put_u64_le(self.index);
        bytes.put_u64_le(self.term);
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
        let flags = bytes.slice(16..24).get_u64_le();
        let timestamp = IggyTimestamp::from(bytes.slice(24..32).get_u64_le());
        let user_id = bytes.slice(32..36).get_u32_le();
        let code = bytes.slice(36..40).get_u32_le();
        let payload_length = bytes.slice(40..44).get_u32_le() as usize;
        let payload = bytes.slice(44..44 + payload_length);
        let context_length = bytes
            .slice(44 + payload_length..48 + payload_length)
            .get_u32_le() as usize;
        let context = bytes.slice(48 + payload_length..48 + payload_length + context_length);
        Ok(StateEntry {
            index,
            term,
            flags,
            timestamp,
            user_id,
            code,
            payload,
            context,
        })
    }
}
