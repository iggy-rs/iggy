use super::send_messages::Partitioning;
use crate::{
    bytes_serializable::BytesSerializable,
    command::{Command, SEND_MESSAGES_CODE},
    error::IggyError,
    identifier::Identifier,
    models::batch::IggyBatch,
    utils::sizeable::Sizeable,
    validatable::Validatable,
};
use bytes::Bytes;

#[derive(Debug, PartialEq, Default)]
pub struct SendMessages {
    pub stream_id: Identifier,
    pub topic_id: Identifier,
    pub partitioning: Partitioning,
    pub batch: IggyBatch,
}

impl std::fmt::Display for SendMessages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|batch_size:{}",
            self.stream_id,
            self.topic_id,
            self.partitioning,
            self.batch.payload.len()
        )
    }
}

impl Command for SendMessages {
    fn code(&self) -> u32 {
        SEND_MESSAGES_CODE
    }
}

impl Validatable<IggyError> for SendMessages {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl BytesSerializable for SendMessages {
    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        if bytes.len() < 11 {
            return Err(IggyError::InvalidCommand);
        }

        todo!();
        /*
        let command = Self {
            stream_id,
            topic_id,
            partitioning: key,
            batch,
        };
        Ok(command)
        */
    }

    fn to_bytes(&self) -> Bytes {
        todo!()
    }
}
