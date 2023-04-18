use crate::handlers::messages::*;
use crate::handlers::streams::*;
use crate::handlers::system::*;
use crate::handlers::topics::*;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::error::Error;
use shared::messages::poll_messages::PollMessages;
use shared::messages::send_message::SendMessage;
use shared::streams::create_stream::CreateStream;
use shared::streams::delete_stream::DeleteStream;
use shared::topics::create_topic::CreateTopic;
use shared::topics::delete_topic::DeleteTopic;
use shared::topics::get_topics::GetTopics;
use std::net::SocketAddr;
use streaming::system::System;
use tokio::net::UdpSocket;
use tracing::{error, info};

/*
  FRAME: | COMMAND |   DATA    |
         | 1 byte  |  n bytes  |

1. PING: | COMMAND |
         | 1 byte  |

2. POLL: | COMMAND |   STREAM  |   TOPIC   |    KIND   |   VALUE   |   COUNT   |
         | 1 byte  |  4 bytes  |  4 bytes  |   1 byte  |  8 bytes  |  4 bytes  |

3. SEND: | COMMAND |   STREAM  |    TOPIC   |    KIND   |    KEY    |  PAYLOAD  |
         | 1 byte  |  4 bytes  |   4 bytes  |   1 byte  |  8 bytes  |  n bytes  |
*/

/*
  RESPONSE: |   STATUS  |   DATA    |
            |   1 byte  |  n bytes  |
*/

const LENGTH: usize = 1;

pub async fn handle(
    request: &[u8],
    socket: &UdpSocket,
    address: SocketAddr,
    system: &mut System,
) -> Result<(), Error> {
    if request.len() < LENGTH {
        return Err(Error::InvalidCommand);
    }

    let command = Command::from_bytes(&request[..LENGTH])?;
    let bytes = &request[LENGTH..];
    try_handle(command, bytes, socket, address, system).await
}

async fn try_handle(
    command: Command,
    bytes: &[u8],
    socket: &UdpSocket,
    address: SocketAddr,
    system: &mut System,
) -> Result<(), Error> {
    info!(
        "Handling command '{:?}' from client: {:?}...",
        command, address
    );
    match command {
        Command::Ping => ping_handler::handle(socket, address).await,
        Command::SendMessage => {
            let command = SendMessage::from_bytes(bytes)?;
            send_message_handler::handle(command, socket, address, system).await
        }
        Command::PollMessages => {
            let command = PollMessages::from_bytes(bytes)?;
            poll_messages_handler::handle(command, socket, address, system).await
        }
        Command::GetStreams => get_streams_handler::handle(socket, address, system).await,
        Command::CreateStream => {
            let command = CreateStream::from_bytes(bytes)?;
            create_stream_handler::handle(command, socket, address, system).await
        }
        Command::DeleteStream => {
            let command = DeleteStream::from_bytes(bytes)?;
            delete_stream_handler::handle(command, socket, address, system).await
        }
        Command::GetTopics => {
            let command = GetTopics::from_bytes(bytes)?;
            get_topics_handler::handle(command, socket, address, system).await
        }
        Command::CreateTopic => {
            let command = CreateTopic::from_bytes(bytes)?;
            create_topic_handler::handle(command, socket, address, system).await
        }
        Command::DeleteTopic => {
            let command = DeleteTopic::from_bytes(bytes)?;
            delete_topic_handler::handle(command, socket, address, system).await
        }
    }
}

pub async fn handle_error(error: Error, socket: &UdpSocket, address: SocketAddr) {
    error!("{}", error);
    if socket
        .send_to(&error.code().to_le_bytes(), address)
        .await
        .is_err()
    {
        error!("Could not send error to client: {:?}", address);
    }
}
