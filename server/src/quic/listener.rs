use crate::binary::command;
use crate::quic::quic_sender::QuicSender;
use crate::server_error::ServerError;
use quinn::Endpoint;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::log::trace;
use tracing::{error, info};

const LISTENERS_COUNT: u32 = 10;
const INITIAL_REQUEST_LENGTH: usize = 5;

pub fn start(endpoint: Endpoint, system: Arc<RwLock<System>>) {
    for _ in 0..LISTENERS_COUNT {
        let endpoint = endpoint.clone();
        let system = system.clone();
        tokio::spawn(async move {
            while let Some(incoming_connection) = endpoint.accept().await {
                info!(
                    "Incoming connection from client: {}",
                    incoming_connection.remote_address()
                );
                let system = system.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(incoming_connection, system).await {
                        error!("Connection has failed: {}", error.to_string())
                    }
                });
            }
        });
    }
}

async fn handle_connection(
    incoming_connection: quinn::Connecting,
    system: Arc<RwLock<System>>,
) -> Result<(), ServerError> {
    let connection = incoming_connection.await?;
    async {
        info!("Client has connected: {}", connection.remote_address());
        loop {
            let stream = connection.accept_bi().await;
            let mut stream = match stream {
                Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                    info!("Connection closed");
                    return Ok(());
                }
                Err(error) => {
                    return Err(error);
                }
                Ok(stream) => stream,
            };

            let request = stream.1.read_to_end(10 * 1024 * 1024).await;
            if request.is_err() {
                error!("Error when reading the QUIC request: {:?}", request);
                continue;
            }

            let request = request.unwrap();
            if request.len() < INITIAL_REQUEST_LENGTH {
                error!("Error when reading the QUIC request command: {:?}", request);
                continue;
            }

            trace!("Trying to read command...");
            let command = Command::from_bytes(&request[..1]);
            if command.is_err() {
                error!(
                    "Error when reading the QUIC request command: {:?}",
                    command.err()
                );
                continue;
            }

            let command = command.unwrap();
            let bytes = &request[INITIAL_REQUEST_LENGTH..];
            trace!(
                "Received command: '{:?}',  payload size: {}, trying to handle...",
                command,
                bytes.len()
            );

            let result = command::handle(
                command,
                bytes,
                &mut QuicSender { send: stream.0 },
                system.clone(),
            )
            .await;
            if result.is_err() {
                error!("Error when handling the QUIC request: {:?}", result.err());
                continue;
            }
        }
    }
    .await?;
    Ok(())
}
