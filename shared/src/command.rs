use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;
use crate::messages::poll_messages::PollMessages;
use crate::messages::send_messages::SendMessages;
use crate::offsets::get_offset::GetOffset;
use crate::offsets::store_offset::StoreOffset;
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use crate::system::kill::Kill;
use crate::system::ping::Ping;
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

pub const KILL: &str = "kill";
pub const PING: &str = "ping";
pub const SEND_MESSAGES: &str = "message.send";
pub const POLL_MESSAGES: &str = "message.poll";
pub const STORE_OFFSET: &str = "offset.store";
pub const GET_OFFSET: &str = "offset.get";
pub const GET_STREAMS: &str = "stream.list";
pub const GET_STREAM: &str = "stream.get";
pub const CREATE_STREAM: &str = "stream.create";
pub const DELETE_STREAM: &str = "stream.delete";
pub const GET_TOPICS: &str = "topic.list";
pub const GET_TOPIC: &str = "topic.get";
pub const CREATE_TOPIC: &str = "topic.create";
pub const DELETE_TOPIC: &str = "topic.delete";

#[derive(Debug, PartialEq)]
pub enum Command {
    Kill(Kill),
    Ping(Ping),
    SendMessages(SendMessages),
    PollMessages(PollMessages),
    GetOffset(GetOffset),
    StoreOffset(StoreOffset),
    GetStream(GetStream),
    GetStreams(GetStreams),
    CreateStream(CreateStream),
    DeleteStream(DeleteStream),
    GetTopic(GetTopic),
    GetTopics(GetTopics),
    CreateTopic(CreateTopic),
    DeleteTopic(DeleteTopic),
}

pub trait CommandPayload: BytesSerializable + Display {}

impl BytesSerializable for Command {
    fn as_bytes(&self) -> Vec<u8> {
        match self {
            Command::Kill(payload) => as_bytes(0, &payload.as_bytes()),
            Command::Ping(payload) => as_bytes(1, &payload.as_bytes()),
            Command::SendMessages(payload) => as_bytes(2, &payload.as_bytes()),
            Command::PollMessages(payload) => as_bytes(3, &payload.as_bytes()),
            Command::StoreOffset(payload) => as_bytes(4, &payload.as_bytes()),
            Command::GetOffset(payload) => as_bytes(5, &payload.as_bytes()),
            Command::GetStream(payload) => as_bytes(10, &payload.as_bytes()),
            Command::GetStreams(payload) => as_bytes(11, &payload.as_bytes()),
            Command::CreateStream(payload) => as_bytes(12, &payload.as_bytes()),
            Command::DeleteStream(payload) => as_bytes(13, &payload.as_bytes()),
            Command::GetTopic(payload) => as_bytes(20, &payload.as_bytes()),
            Command::GetTopics(payload) => as_bytes(21, &payload.as_bytes()),
            Command::CreateTopic(payload) => as_bytes(22, &payload.as_bytes()),
            Command::DeleteTopic(payload) => as_bytes(23, &payload.as_bytes()),
        }
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let command = bytes[0];
        let payload = &bytes[1..];
        match command {
            0 => Ok(Command::Kill(Kill::from_bytes(payload)?)),
            1 => Ok(Command::Ping(Ping::from_bytes(payload)?)),
            2 => Ok(Command::SendMessages(SendMessages::from_bytes(payload)?)),
            3 => Ok(Command::PollMessages(PollMessages::from_bytes(payload)?)),
            4 => Ok(Command::StoreOffset(StoreOffset::from_bytes(payload)?)),
            5 => Ok(Command::GetOffset(GetOffset::from_bytes(payload)?)),
            10 => Ok(Command::GetStream(GetStream::from_bytes(payload)?)),
            11 => Ok(Command::GetStreams(GetStreams::from_bytes(payload)?)),
            12 => Ok(Command::CreateStream(CreateStream::from_bytes(payload)?)),
            13 => Ok(Command::DeleteStream(DeleteStream::from_bytes(payload)?)),
            20 => Ok(Command::GetTopic(GetTopic::from_bytes(payload)?)),
            21 => Ok(Command::GetTopics(GetTopics::from_bytes(payload)?)),
            22 => Ok(Command::CreateTopic(CreateTopic::from_bytes(payload)?)),
            23 => Ok(Command::DeleteTopic(DeleteTopic::from_bytes(payload)?)),
            _ => Err(Error::InvalidCommand),
        }
    }
}

fn as_bytes(command: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1 + payload.len());
    bytes.extend([command]);
    bytes.extend(payload);
    bytes
}

impl FromStr for Command {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (command, payload) = input.split_once('|').unwrap_or((input, ""));
        match command {
            KILL => Ok(Command::Kill(Kill::from_str(payload)?)),
            PING => Ok(Command::Ping(Ping::from_str(payload)?)),
            SEND_MESSAGES => Ok(Command::SendMessages(SendMessages::from_str(payload)?)),
            POLL_MESSAGES => Ok(Command::PollMessages(PollMessages::from_str(payload)?)),
            STORE_OFFSET => Ok(Command::StoreOffset(StoreOffset::from_str(payload)?)),
            GET_OFFSET => Ok(Command::GetOffset(GetOffset::from_str(payload)?)),
            GET_STREAM => Ok(Command::GetStream(GetStream::from_str(payload)?)),
            GET_STREAMS => Ok(Command::GetStreams(GetStreams::from_str(payload)?)),
            CREATE_STREAM => Ok(Command::CreateStream(CreateStream::from_str(payload)?)),
            DELETE_STREAM => Ok(Command::DeleteStream(DeleteStream::from_str(payload)?)),
            GET_TOPIC => Ok(Command::GetTopic(GetTopic::from_str(payload)?)),
            GET_TOPICS => Ok(Command::GetTopics(GetTopics::from_str(payload)?)),
            CREATE_TOPIC => Ok(Command::CreateTopic(CreateTopic::from_str(payload)?)),
            DELETE_TOPIC => Ok(Command::DeleteTopic(DeleteTopic::from_str(payload)?)),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl Display for Command {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Kill(payload) => write!(formatter, "{}|{}", KILL, payload),
            Command::Ping(payload) => write!(formatter, "{}|{}", PING, payload),
            Command::GetStream(payload) => write!(formatter, "{}|{}", GET_STREAM, payload),
            Command::GetStreams(payload) => write!(formatter, "{}|{}", GET_STREAMS, payload),
            Command::CreateStream(payload) => write!(formatter, "{}|{}", CREATE_STREAM, payload),
            Command::DeleteStream(payload) => write!(formatter, "{}|{}", DELETE_STREAM, payload),
            Command::GetTopic(payload) => write!(formatter, "{}|{}", GET_TOPIC, payload),
            Command::GetTopics(payload) => write!(formatter, "{}|{}", GET_TOPICS, payload),
            Command::CreateTopic(payload) => write!(formatter, "{}|{}", CREATE_TOPIC, payload),
            Command::DeleteTopic(payload) => write!(formatter, "{}|{}", DELETE_TOPIC, payload),
            Command::PollMessages(payload) => write!(formatter, "{}|{}", POLL_MESSAGES, payload),
            Command::SendMessages(payload) => write!(formatter, "{}|{}", SEND_MESSAGES, payload),
            Command::StoreOffset(payload) => write!(formatter, "{}|{}", STORE_OFFSET, payload),
            Command::GetOffset(payload) => write!(formatter, "{}|{}", GET_OFFSET, payload),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes_and_deserialized_from_bytes() {
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::Kill(Kill::default()),
            0,
            &Kill::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::Ping(Ping::default()),
            1,
            &Ping::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::SendMessages(SendMessages::default()),
            2,
            &SendMessages::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::PollMessages(PollMessages::default()),
            3,
            &PollMessages::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::StoreOffset(StoreOffset::default()),
            4,
            &StoreOffset::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::GetOffset(GetOffset::default()),
            5,
            &GetOffset::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::GetStream(GetStream::default()),
            10,
            &GetStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::GetStreams(GetStreams::default()),
            11,
            &GetStreams::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::CreateStream(CreateStream::default()),
            12,
            &CreateStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::DeleteStream(DeleteStream::default()),
            13,
            &DeleteStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::GetTopic(GetTopic::default()),
            20,
            &GetTopic::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::GetTopics(GetTopics::default()),
            21,
            &GetTopics::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::CreateTopic(CreateTopic::default()),
            22,
            &CreateTopic::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::DeleteTopic(DeleteTopic::default()),
            23,
            &DeleteTopic::default(),
        );
    }

    #[test]
    fn should_be_read_from_string() {
        assert_read_from_string(&Command::Kill(Kill::default()), KILL, &Kill::default());
        assert_read_from_string(&Command::Ping(Ping::default()), PING, &Ping::default());
        assert_read_from_string(
            &Command::SendMessages(SendMessages::default()),
            SEND_MESSAGES,
            &SendMessages::default(),
        );
        assert_read_from_string(
            &Command::PollMessages(PollMessages::default()),
            POLL_MESSAGES,
            &PollMessages::default(),
        );
        assert_read_from_string(
            &Command::StoreOffset(StoreOffset::default()),
            STORE_OFFSET,
            &StoreOffset::default(),
        );
        assert_read_from_string(
            &Command::GetOffset(GetOffset::default()),
            GET_OFFSET,
            &GetOffset::default(),
        );
        assert_read_from_string(
            &Command::GetStream(GetStream::default()),
            GET_STREAM,
            &GetStream::default(),
        );
        assert_read_from_string(
            &Command::GetStreams(GetStreams::default()),
            GET_STREAMS,
            &GetStreams::default(),
        );
        assert_read_from_string(
            &Command::CreateStream(CreateStream::default()),
            CREATE_STREAM,
            &CreateStream::default(),
        );
        assert_read_from_string(
            &Command::DeleteStream(DeleteStream::default()),
            DELETE_STREAM,
            &DeleteStream::default(),
        );
        assert_read_from_string(
            &Command::GetTopic(GetTopic::default()),
            GET_TOPIC,
            &GetTopic::default(),
        );
        assert_read_from_string(
            &Command::GetTopics(GetTopics::default()),
            GET_TOPICS,
            &GetTopics::default(),
        );
        assert_read_from_string(
            &Command::CreateTopic(CreateTopic::default()),
            CREATE_TOPIC,
            &CreateTopic::default(),
        );
        assert_read_from_string(
            &Command::DeleteTopic(DeleteTopic::default()),
            DELETE_TOPIC,
            &DeleteTopic::default(),
        );
    }

    fn assert_serialized_as_bytes_and_deserialized_from_bytes(
        command: &Command,
        command_id: u8,
        payload: &dyn CommandPayload,
    ) {
        assert_serialized_as_bytes(command, command_id, payload);
        assert_deserialized_from_bytes(command, command_id, payload);
    }

    fn assert_serialized_as_bytes(command: &Command, command_id: u8, payload: &dyn CommandPayload) {
        let payload = payload.as_bytes();
        let mut bytes = Vec::with_capacity(1 + payload.len());
        bytes.extend([command_id]);
        bytes.extend(payload);
        assert_eq!(command.as_bytes(), bytes);
    }

    fn assert_deserialized_from_bytes(
        command: &Command,
        command_id: u8,
        payload: &dyn CommandPayload,
    ) {
        let payload = payload.as_bytes();
        let mut bytes = Vec::with_capacity(1 + payload.len());
        bytes.extend([command_id]);
        bytes.extend(payload);
        assert_eq!(&Command::from_bytes(&bytes).unwrap(), command);
    }

    fn assert_read_from_string(
        command: &Command,
        command_name: &str,
        payload: &dyn CommandPayload,
    ) {
        let payload = payload.to_string();
        let mut string = String::with_capacity(command_name.len() + payload.len());
        string.push_str(command_name);
        if !payload.is_empty() {
            string.push('|');
            string.push_str(&payload);
        }
        assert_eq!(&Command::from_str(&string).unwrap(), command);
    }
}
