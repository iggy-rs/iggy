use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

pub const KILL: &str = "kill";
pub const PING: &str = "ping";
pub const SEND_MESSAGES: &str = "message.send";
pub const POLL_MESSAGES: &str = "message.poll";
pub const STORE_OFFSET: &str = "offset.store";
pub const GET_STREAMS: &str = "stream.list";
pub const CREATE_STREAM: &str = "stream.create";
pub const DELETE_STREAM: &str = "stream.delete";
pub const GET_TOPICS: &str = "topic.list";
pub const CREATE_TOPIC: &str = "topic.create";
pub const DELETE_TOPIC: &str = "topic.delete";

#[derive(Debug, PartialEq)]
pub enum Command {
    Kill,
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
            Command::Kill => [0].to_vec(),
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
            [0] => Ok(Command::Kill),
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
            KILL => Ok(Command::Kill),
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
            Command::Kill => write!(formatter, "{}", KILL),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        assert_eq!(Command::Kill.as_bytes(), [0]);
        assert_eq!(Command::Ping.as_bytes(), [1]);
        assert_eq!(Command::SendMessages.as_bytes(), [2]);
        assert_eq!(Command::PollMessages.as_bytes(), [3]);
        assert_eq!(Command::StoreOffset.as_bytes(), [4]);
        assert_eq!(Command::GetStreams.as_bytes(), [10]);
        assert_eq!(Command::CreateStream.as_bytes(), [11]);
        assert_eq!(Command::DeleteStream.as_bytes(), [12]);
        assert_eq!(Command::GetTopics.as_bytes(), [20]);
        assert_eq!(Command::CreateTopic.as_bytes(), [21]);
        assert_eq!(Command::DeleteTopic.as_bytes(), [22]);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        assert_eq!(Command::from_bytes(&[0]).unwrap(), Command::Kill);
        assert_eq!(Command::from_bytes(&[1]).unwrap(), Command::Ping);
        assert_eq!(Command::from_bytes(&[2]).unwrap(), Command::SendMessages);
        assert_eq!(Command::from_bytes(&[3]).unwrap(), Command::PollMessages);
        assert_eq!(Command::from_bytes(&[4]).unwrap(), Command::StoreOffset);
        assert_eq!(Command::from_bytes(&[10]).unwrap(), Command::GetStreams);
        assert_eq!(Command::from_bytes(&[11]).unwrap(), Command::CreateStream);
        assert_eq!(Command::from_bytes(&[12]).unwrap(), Command::DeleteStream);
        assert_eq!(Command::from_bytes(&[20]).unwrap(), Command::GetTopics);
        assert_eq!(Command::from_bytes(&[21]).unwrap(), Command::CreateTopic);
        assert_eq!(Command::from_bytes(&[22]).unwrap(), Command::DeleteTopic);
    }

    #[test]
    fn should_be_read_from_string() {
        assert_eq!(Command::from_str(KILL).unwrap(), Command::Kill);
        assert_eq!(Command::from_str(PING).unwrap(), Command::Ping);
        assert_eq!(
            Command::from_str(SEND_MESSAGES).unwrap(),
            Command::SendMessages
        );
        assert_eq!(
            Command::from_str(POLL_MESSAGES).unwrap(),
            Command::PollMessages
        );
        assert_eq!(
            Command::from_str(STORE_OFFSET).unwrap(),
            Command::StoreOffset
        );
        assert_eq!(Command::from_str(GET_STREAMS).unwrap(), Command::GetStreams);
        assert_eq!(
            Command::from_str(CREATE_STREAM).unwrap(),
            Command::CreateStream
        );
        assert_eq!(
            Command::from_str(DELETE_STREAM).unwrap(),
            Command::DeleteStream
        );
        assert_eq!(Command::from_str(GET_TOPICS).unwrap(), Command::GetTopics);
        assert_eq!(
            Command::from_str(CREATE_TOPIC).unwrap(),
            Command::CreateTopic
        );
        assert_eq!(
            Command::from_str(DELETE_TOPIC).unwrap(),
            Command::DeleteTopic
        );
    }
}
