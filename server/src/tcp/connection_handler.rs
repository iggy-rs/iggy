use crate::binary::command;
use crate::binary::sender::Sender;
use crate::server_error::ServerError;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::Command;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use streaming::clients::client_manager::Transport;
use streaming::systems::system::System;
use streaming::users::user_context::UserContext;
use tokio::sync::RwLock;
use tracing::{error, info, trace};

const INITIAL_BYTES_LENGTH: usize = 4;

pub(crate) async fn handle_connection(
    address: &SocketAddr,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), ServerError> {
    // TODO: Authenticate the user and map ID
    let user_id = 1;
    let client_id = system
        .read()
        .await
        .add_client(address, Transport::Tcp)
        .await;
    let user_context = UserContext { client_id, user_id };
    let mut initial_buffer = [0u8; INITIAL_BYTES_LENGTH];

    loop {
        let read_length = sender.read(&mut initial_buffer).await?;
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
        sender.read(&mut command_buffer).await?;
        let command = Command::from_bytes(&command_buffer)?;
        trace!(
            "Received a TCP command: {}, payload size: {}",
            command,
            length
        );
        let result = command::handle(&command, sender, &user_context, system.clone()).await;
        if result.is_err() {
            error!("Error when handling the TCP request: {:?}", result.err());
            continue;
        }
        trace!("Sent a TCP response.");
    }
}

pub(crate) fn handle_error(error: ServerError) {
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
        ServerError::SdkError(_) => {}
        _ => {
            error!("Connection has failed 2: {}", error.to_string())
        }
    }
}
