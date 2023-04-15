use crate::handlers::*;
use std::net::SocketAddr;
use streaming::error::Error;
use streaming::system::System;
use tokio::net::UdpSocket;
use tracing::{error, info};

const LENGTH: usize = 1;

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

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping,
    Poll,
    Send,
    // GetStreams,
    CreateStream,
    // DeleteStream,
    GetTopics,
    CreateTopic,
    DeleteTopic,
}

impl Command {
    pub async fn try_handle(
        request: &[u8],
        socket: &UdpSocket,
        address: SocketAddr,
        system: &mut System,
    ) {
        if request.len() < LENGTH {
            handle_error(Error::InvalidCommand, socket, address).await;
            return;
        }

        let received_command = &request[..LENGTH];
        let input = &request[LENGTH..];
        let command = Command::from(received_command);
        if command.is_none() {
            handle_error(Error::InvalidCommand, socket, address).await;
            return;
        }

        let result = command
            .unwrap()
            .handle(input, socket, address, system)
            .await;

        if result.is_err() {
            handle_error(result.err().unwrap(), socket, address).await;
        }
    }

    fn from(command: &[u8]) -> Option<Command> {
        match command {
            ping_handler::COMMAND => Some(Command::Ping),
            poll_handler::COMMAND => Some(Command::Poll),
            send_handler::COMMAND => Some(Command::Send),
            create_stream_handler::COMMAND => Some(Command::CreateStream),
            get_topics_handler::COMMAND => Some(Command::GetTopics),
            create_topic_handler::COMMAND => Some(Command::CreateTopic),
            delete_topic_handler::COMMAND => Some(Command::DeleteTopic),
            _ => None,
        }
    }

    async fn handle(
        &self,
        input: &[u8],
        socket: &UdpSocket,
        address: SocketAddr,
        system: &mut System,
    ) -> Result<(), Error> {
        info!(
            "Handling command '{:?}' from client: {:?}...",
            self, address
        );
        match self {
            Command::Ping => ping_handler::handle(socket, address).await,
            Command::Poll => poll_handler::handle(input, socket, address, system).await,
            Command::Send => send_handler::handle(input, socket, address, system).await,
            Command::CreateStream => {
                create_stream_handler::handle(input, socket, address, system).await
            }
            Command::GetTopics => get_topics_handler::handle(input, socket, address, system).await,
            Command::CreateTopic => {
                create_topic_handler::handle(input, socket, address, system).await
            }
            Command::DeleteTopic => {
                delete_topic_handler::handle(input, socket, address, system).await
            }
        }
    }
}

async fn handle_error(error: Error, socket: &UdpSocket, address: SocketAddr) {
    error!("{}", error);
    if socket
        .send_to(&error.code().to_le_bytes(), address)
        .await
        .is_err()
    {
        error!("Could not send error to client: {:?}", address);
    }
}
