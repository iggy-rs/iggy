use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping,
    SendMessage,
    PollMessages,
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
            Command::SendMessage => [2].to_vec(),
            Command::PollMessages => [3].to_vec(),
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
            [2] => Ok(Command::SendMessage),
            [3] => Ok(Command::PollMessages),
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
            "ping" => Ok(Command::Ping),
            "message.send" => Ok(Command::SendMessage),
            "message.poll" => Ok(Command::PollMessages),
            "stream.list" => Ok(Command::GetStreams),
            "stream.create" => Ok(Command::CreateStream),
            "stream.delete" => Ok(Command::DeleteStream),
            "topic.list" => Ok(Command::GetTopics),
            "topic.create" => Ok(Command::CreateTopic),
            "topic.delete" => Ok(Command::DeleteTopic),
            _ => Err(()),
        }
    }
}

impl Display for Command {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Ping => write!(formatter, "ping"),
            Command::GetStreams => write!(formatter, "stream.list"),
            Command::CreateStream => write!(formatter, "stream.create"),
            Command::DeleteStream => write!(formatter, "stream.delete"),
            Command::GetTopics => write!(formatter, "topic.list"),
            Command::CreateTopic => write!(formatter, "topic.create"),
            Command::DeleteTopic => write!(formatter, "topic.delete"),
            Command::PollMessages => write!(formatter, "message.poll"),
            Command::SendMessage => write!(formatter, "message.send"),
        }
    }
}
