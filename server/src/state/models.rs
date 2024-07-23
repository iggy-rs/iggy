use bytes::{BufMut, Bytes, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::Command;
use iggy::error::IggyError;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use iggy::validatable::Validatable;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::str::from_utf8;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct CreatePersonalAccessTokenWithHash {
    pub command: CreatePersonalAccessToken,
    pub hash: String,
}

impl Validatable<IggyError> for CreatePersonalAccessTokenWithHash {
    fn validate(&self) -> Result<(), IggyError> {
        self.command.validate()
    }
}

impl Command for CreatePersonalAccessTokenWithHash {
    fn code(&self) -> u32 {
        self.command.code()
    }
}

impl BytesSerializable for CreatePersonalAccessTokenWithHash {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        let command_bytes = self.command.to_bytes();
        bytes.put_u32_le(command_bytes.len() as u32);
        bytes.put_slice(&command_bytes);
        bytes.put_u32_le(self.hash.len() as u32);
        bytes.put_slice(self.hash.as_bytes());
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let mut position = 0;
        let command_length = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        position += 4;
        let command_bytes = bytes.slice(position..position + command_length as usize);
        position += command_length as usize;
        let command = CreatePersonalAccessToken::from_bytes(command_bytes)?;
        let hash_length = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        position += 4;
        let hash = from_utf8(&bytes[position..position + hash_length as usize])?.to_string();
        Ok(Self { command, hash })
    }
}

impl Display for CreatePersonalAccessTokenWithHash {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CreatePersonalAccessTokenWithHash {{ command: {}, hash: {} }}",
            self.command, self.hash
        )
    }
}
