use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;
use crate::groups::create_group::CreateGroup;
use crate::groups::delete_group::DeleteGroup;
use crate::messages::poll_messages::PollMessages;
use crate::messages::send_messages::SendMessages;
use crate::offsets::get_offset::GetOffset;
use crate::offsets::store_offset::StoreOffset;
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use crate::system::get_clients::GetClients;
use crate::system::kill::Kill;
use crate::system::ping::Ping;
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

pub const KILL: &str = "kill";
pub const KILL_CODE: u8 = 0;
pub const PING: &str = "ping";
pub const PING_CODE: u8 = 1;
pub const GET_CLIENTS: &str = "client.list";
pub const GET_CLIENTS_CODE: u8 = 2;
pub const SEND_MESSAGES: &str = "message.send";
pub const SEND_MESSAGES_CODE: u8 = 10;
pub const POLL_MESSAGES: &str = "message.poll";
pub const POLL_MESSAGES_CODE: u8 = 11;
pub const STORE_OFFSET: &str = "offset.store";
pub const STORE_OFFSET_CODE: u8 = 12;
pub const GET_OFFSET: &str = "offset.get";
pub const GET_OFFSET_CODE: u8 = 13;
pub const GET_STREAM: &str = "stream.get";
pub const GET_STREAM_CODE: u8 = 20;
pub const GET_STREAMS: &str = "stream.list";
pub const GET_STREAMS_CODE: u8 = 21;
pub const CREATE_STREAM: &str = "stream.create";
pub const CREATE_STREAM_CODE: u8 = 22;
pub const DELETE_STREAM: &str = "stream.delete";
pub const DELETE_STREAM_CODE: u8 = 23;
pub const GET_TOPIC: &str = "topic.get";
pub const GET_TOPIC_CODE: u8 = 30;
pub const GET_TOPICS: &str = "topic.list";
pub const GET_TOPICS_CODE: u8 = 31;
pub const CREATE_TOPIC: &str = "topic.create";
pub const CREATE_TOPIC_CODE: u8 = 32;
pub const DELETE_TOPIC: &str = "topic.delete";
pub const DELETE_TOPIC_CODE: u8 = 33;
pub const CREATE_GROUP: &str = "group.create";
pub const CREATE_GROUP_CODE: u8 = 42;
pub const DELETE_GROUP: &str = "group.delete";
pub const DELETE_GROUP_CODE: u8 = 43;

#[derive(Debug, PartialEq)]
pub enum Command {
    Kill(Kill),
    Ping(Ping),
    GetClients(GetClients),
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
    CreateGroup(CreateGroup),
    DeleteGroup(DeleteGroup),
}

pub trait CommandPayload: BytesSerializable + Display {}

impl BytesSerializable for Command {
    fn as_bytes(&self) -> Vec<u8> {
        match self {
            Command::Kill(payload) => as_bytes(KILL_CODE, &payload.as_bytes()),
            Command::Ping(payload) => as_bytes(PING_CODE, &payload.as_bytes()),
            Command::GetClients(payload) => as_bytes(GET_CLIENTS_CODE, &payload.as_bytes()),
            Command::SendMessages(payload) => as_bytes(SEND_MESSAGES_CODE, &payload.as_bytes()),
            Command::PollMessages(payload) => as_bytes(POLL_MESSAGES_CODE, &payload.as_bytes()),
            Command::StoreOffset(payload) => as_bytes(STORE_OFFSET_CODE, &payload.as_bytes()),
            Command::GetOffset(payload) => as_bytes(GET_OFFSET_CODE, &payload.as_bytes()),
            Command::GetStream(payload) => as_bytes(GET_STREAM_CODE, &payload.as_bytes()),
            Command::GetStreams(payload) => as_bytes(GET_STREAMS_CODE, &payload.as_bytes()),
            Command::CreateStream(payload) => as_bytes(CREATE_STREAM_CODE, &payload.as_bytes()),
            Command::DeleteStream(payload) => as_bytes(DELETE_STREAM_CODE, &payload.as_bytes()),
            Command::GetTopic(payload) => as_bytes(GET_TOPIC_CODE, &payload.as_bytes()),
            Command::GetTopics(payload) => as_bytes(GET_TOPICS_CODE, &payload.as_bytes()),
            Command::CreateTopic(payload) => as_bytes(CREATE_TOPIC_CODE, &payload.as_bytes()),
            Command::DeleteTopic(payload) => as_bytes(DELETE_TOPIC_CODE, &payload.as_bytes()),
            Command::CreateGroup(payload) => as_bytes(CREATE_GROUP_CODE, &payload.as_bytes()),
            Command::DeleteGroup(payload) => as_bytes(DELETE_GROUP_CODE, &payload.as_bytes()),
        }
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let command = bytes[0];
        let payload = &bytes[1..];
        match command {
            KILL_CODE => Ok(Command::Kill(Kill::from_bytes(payload)?)),
            PING_CODE => Ok(Command::Ping(Ping::from_bytes(payload)?)),
            GET_CLIENTS_CODE => Ok(Command::GetClients(GetClients::from_bytes(payload)?)),
            SEND_MESSAGES_CODE => Ok(Command::SendMessages(SendMessages::from_bytes(payload)?)),
            POLL_MESSAGES_CODE => Ok(Command::PollMessages(PollMessages::from_bytes(payload)?)),
            STORE_OFFSET_CODE => Ok(Command::StoreOffset(StoreOffset::from_bytes(payload)?)),
            GET_OFFSET_CODE => Ok(Command::GetOffset(GetOffset::from_bytes(payload)?)),
            GET_STREAM_CODE => Ok(Command::GetStream(GetStream::from_bytes(payload)?)),
            GET_STREAMS_CODE => Ok(Command::GetStreams(GetStreams::from_bytes(payload)?)),
            CREATE_STREAM_CODE => Ok(Command::CreateStream(CreateStream::from_bytes(payload)?)),
            DELETE_STREAM_CODE => Ok(Command::DeleteStream(DeleteStream::from_bytes(payload)?)),
            GET_TOPIC_CODE => Ok(Command::GetTopic(GetTopic::from_bytes(payload)?)),
            GET_TOPICS_CODE => Ok(Command::GetTopics(GetTopics::from_bytes(payload)?)),
            CREATE_TOPIC_CODE => Ok(Command::CreateTopic(CreateTopic::from_bytes(payload)?)),
            DELETE_TOPIC_CODE => Ok(Command::DeleteTopic(DeleteTopic::from_bytes(payload)?)),
            CREATE_GROUP_CODE => Ok(Command::CreateGroup(CreateGroup::from_bytes(payload)?)),
            DELETE_GROUP_CODE => Ok(Command::DeleteGroup(DeleteGroup::from_bytes(payload)?)),
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
            GET_CLIENTS => Ok(Command::GetClients(GetClients::from_str(payload)?)),
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
            CREATE_GROUP => Ok(Command::CreateGroup(CreateGroup::from_str(payload)?)),
            DELETE_GROUP => Ok(Command::DeleteGroup(DeleteGroup::from_str(payload)?)),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl Display for Command {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Kill(payload) => write!(formatter, "{}|{}", KILL, payload),
            Command::Ping(payload) => write!(formatter, "{}|{}", PING, payload),
            Command::GetClients(payload) => write!(formatter, "{}|{}", GET_CLIENTS, payload),
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
            Command::CreateGroup(payload) => write!(formatter, "{}|{}", CREATE_GROUP, payload),
            Command::DeleteGroup(payload) => write!(formatter, "{}|{}", DELETE_GROUP, payload),
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
            KILL_CODE,
            &Kill::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::Ping(Ping::default()),
            PING_CODE,
            &Ping::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::GetClients(GetClients::default()),
            GET_CLIENTS_CODE,
            &GetClients::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::SendMessages(SendMessages::default()),
            SEND_MESSAGES_CODE,
            &SendMessages::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::PollMessages(PollMessages::default()),
            POLL_MESSAGES_CODE,
            &PollMessages::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::StoreOffset(StoreOffset::default()),
            STORE_OFFSET_CODE,
            &StoreOffset::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::GetOffset(GetOffset::default()),
            GET_OFFSET_CODE,
            &GetOffset::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::GetStream(GetStream::default()),
            GET_STREAM_CODE,
            &GetStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::GetStreams(GetStreams::default()),
            GET_STREAMS_CODE,
            &GetStreams::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::CreateStream(CreateStream::default()),
            CREATE_STREAM_CODE,
            &CreateStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::DeleteStream(DeleteStream::default()),
            DELETE_STREAM_CODE,
            &DeleteStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::GetTopic(GetTopic::default()),
            GET_TOPIC_CODE,
            &GetTopic::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::GetTopics(GetTopics::default()),
            GET_TOPICS_CODE,
            &GetTopics::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::CreateTopic(CreateTopic::default()),
            CREATE_TOPIC_CODE,
            &CreateTopic::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::DeleteTopic(DeleteTopic::default()),
            DELETE_TOPIC_CODE,
            &DeleteTopic::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::CreateGroup(CreateGroup::default()),
            CREATE_GROUP_CODE,
            &CreateGroup::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &Command::DeleteGroup(DeleteGroup::default()),
            DELETE_GROUP_CODE,
            &DeleteGroup::default(),
        );
    }

    #[test]
    fn should_be_read_from_string() {
        assert_read_from_string(&Command::Kill(Kill::default()), KILL, &Kill::default());
        assert_read_from_string(&Command::Ping(Ping::default()), PING, &Ping::default());
        assert_read_from_string(
            &Command::GetClients(GetClients::default()),
            GET_CLIENTS,
            &GetClients::default(),
        );
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
        assert_read_from_string(
            &Command::CreateGroup(CreateGroup::default()),
            CREATE_GROUP,
            &CreateGroup::default(),
        );
        assert_read_from_string(
            &Command::DeleteGroup(DeleteGroup::default()),
            DELETE_GROUP,
            &DeleteGroup::default(),
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
