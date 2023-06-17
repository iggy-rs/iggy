use crate::binary::command;
use crate::server_error::ServerError;
use crate::tcp::tcp_sender::TcpSender;
use sdk::bytes_serializable::BytesSerializable;
use sdk::command::Command;
use std::io::ErrorKind;
use std::sync::Arc;
use streaming::system::System;
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
                Ok((stream, addr)) => {
                    info!("Accepted new TCP connection: {}", addr);
                    let system = system.clone();
                    tokio::spawn(async move {
                        if let Err(error) = handle_connection(stream, system).await {
                            handle_error(error);
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
        let result = command::handle(command, &mut sender, system.clone()).await;
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
