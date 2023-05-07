use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

const PING: &str = "ping";
const SEND_MESSAGES: &str = "message.send";
const POLL_MESSAGES: &str = "message.poll";
const STORE_OFFSET: &str = "offset.store";
const GET_STREAMS: &str = "stream.list";
const CREATE_STREAM: &str = "stream.create";
const DELETE_STREAM: &str = "stream.delete";
const GET_TOPICS: &str = "topic.list";
const CREATE_TOPIC: &str = "topic.create";
const DELETE_TOPIC: &str = "topic.delete";

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping,
    SendMessages,
    PollMessages,
    StoreOffset,
    GetStreams,
    CreateStream,
    DeleteStream,
    GetTopics,
    CreateTopic,
    DeleteTopic,
}

impl BytesSerializable for Command {
    type Type = Command;
    fn as_bytes(&self) -> Vec<u8> {
        match self {
            Command::Ping => [1].to_vec(),
            Command::SendMessages => [2].to_vec(),
            Command::PollMessages => [3].to_vec(),
            Command::StoreOffset => [4].to_vec(),
            Command::GetStreams => [10].to_vec(),
            Command::CreateStream => [11].to_vec(),
            Command::DeleteStream => [12].to_vec(),
            Command::GetTopics => [20].to_vec(),
            Command::CreateTopic => [21].to_vec(),
            Command::DeleteTopic => [22].to_vec(),
        }
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        match bytes {
            [1] => Ok(Command::Ping),
            [2] => Ok(Command::SendMessages),
            [3] => Ok(Command::PollMessages),
            [4] => Ok(Command::StoreOffset),
            [10] => Ok(Command::GetStreams),
            [11] => Ok(Command::CreateStream),
            [12] => Ok(Command::DeleteStream),
            [20] => Ok(Command::GetTopics),
            [21] => Ok(Command::CreateTopic),
            [22] => Ok(Command::DeleteTopic),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl FromStr for Command {
    type Err = ();
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            PING => Ok(Command::Ping),
            SEND_MESSAGES => Ok(Command::SendMessages),
            POLL_MESSAGES => Ok(Command::PollMessages),
            STORE_OFFSET => Ok(Command::StoreOffset),
            GET_STREAMS => Ok(Command::GetStreams),
            CREATE_STREAM => Ok(Command::CreateStream),
            DELETE_STREAM => Ok(Command::DeleteStream),
            GET_TOPICS => Ok(Command::GetTopics),
            CREATE_TOPIC => Ok(Command::CreateTopic),
            DELETE_TOPIC => Ok(Command::DeleteTopic),
            _ => Err(()),
        }
    }
}

impl Display for Command {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Ping => write!(formatter, "{}", PING),
            Command::GetStreams => write!(formatter, "{}", GET_STREAMS),
            Command::CreateStream => write!(formatter, "{}", CREATE_STREAM),
            Command::DeleteStream => write!(formatter, "{}", DELETE_STREAM),
            Command::GetTopics => write!(formatter, "{}", GET_TOPICS),
            Command::CreateTopic => write!(formatter, "{}", CREATE_TOPIC),
            Command::DeleteTopic => write!(formatter, "{}", DELETE_TOPIC),
            Command::PollMessages => write!(formatter, "{}", POLL_MESSAGES),
            Command::SendMessages => write!(formatter, "{}", SEND_MESSAGES),
            Command::StoreOffset => write!(formatter, "{}", STORE_OFFSET),
        }
    }
}
