use crate::binary::client_context::ClientContext;
use crate::binary::command;
use crate::server_error::ServerError;
use crate::tcp::tcp_sender::TcpSender;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::Command;
use std::io::ErrorKind;
use std::sync::Arc;
use streaming::clients::client_manager::Transport;
use streaming::systems::system::System;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{error, info, trace};

const INITIAL_BYTES_LENGTH: usize = 4;

pub fn start(address: &str, system: Arc<RwLock<System>>) {
    let address = address.to_string();
    tokio::spawn(async move {
        let listener = TcpListener::bind(address).await;
        if listener.is_err() {
            panic!("Unable to start TCP server.");
        }

        let listener = listener.unwrap();
        loop {
            match listener.accept().await {
                Ok((stream, address)) => {
                    info!("Accepted new TCP connection: {}", address);
                    let system = system.clone();
                    tokio::spawn(async move {
                        if let Err(error) = handle_connection(stream, system.clone()).await {
                            handle_error(error);
                            system.read().await.delete_client(&address).await;
                        }
                    });
                }
                Err(error) => error!("Unable to accept TCP socket, error: {}", error),
            }
        }
    });
}

async fn handle_connection(
    stream: TcpStream,
    system: Arc<RwLock<System>>,
) -> Result<(), ServerError> {
    let address = stream.peer_addr()?;
    let client_id = system
        .read()
        .await
        .add_client(&address, Transport::Tcp)
        .await;
    let client_context = ClientContext { client_id };
    let mut sender = TcpSender { stream };
    let mut initial_buffer = [0u8; INITIAL_BYTES_LENGTH];

    loop {
        let read_length = sender.stream.read_exact(&mut initial_buffer).await?;
        if read_length != INITIAL_BYTES_LENGTH {
            error!(
                "Unable to read the TCP request length, expected: {} bytes, received: {} bytes.",
                INITIAL_BYTES_LENGTH, read_length
            );
            continue;
        }

        let length = u32::from_le_bytes(initial_buffer);
        trace!("Received a TCP request, length: {}", length);
        let mut command_buffer = vec![0u8; length as usize];
        sender.stream.read_exact(&mut command_buffer).await?;
        let command = Command::from_bytes(&command_buffer)?;
        trace!(
            "Received a TCP command: {}, payload size: {}",
            command,
            length
        );
        let result = command::handle(&command, &mut sender, &client_context, system.clone()).await;
        if result.is_err() {
            error!("Error when handling the TCP request: {:?}", result.err());
            continue;
        }
        trace!("Sent a TCP response.");
    }
}

fn handle_error(error: ServerError) {
    match error {
        ServerError::IoError(error) => match error.kind() {
            ErrorKind::UnexpectedEof => {
                info!("Connection has been closed.")
            }
            ErrorKind::ConnectionAborted => {
                info!("Connection has been aborted.")
            }
            ErrorKind::ConnectionRefused => {
                info!("Connection has been refused.")
            }
            ErrorKind::ConnectionReset => {
                info!("Connection has been reset.")
            }
            _ => {
                error!("Connection has failed: {}", error.to_string())
            }
        },
        _ => {
            error!("Connection has failed: {}", error.to_string())
        }
    }
}
