use crate::binary::command;
use crate::binary::sender::Sender;
use crate::command::ServerCommand;
use crate::server_error::ServerError;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use bytes::{BufMut, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::validatable::Validatable;
use std::io::ErrorKind;
use std::sync::Arc;
use tracing::{debug, error, info};

const INITIAL_BYTES_LENGTH: usize = 4;

pub(crate) async fn handle_connection(
    session: Arc<Session>,
    sender: &mut dyn Sender,
    system: SharedSystem,
) -> Result<(), ServerError> {
    let mut initial_buffer = [0u8; INITIAL_BYTES_LENGTH];
    loop {
        let read_length = match sender.read(&mut initial_buffer).await {
            Ok(read_length) => read_length,
            Err(error) => {
                if error.as_code() == IggyError::ConnectionClosed.as_code() {
                    return Err(ServerError::from(error));
                } else {
                    sender.send_error_response(error).await?;
                    continue;
                }
            }
        };

        if read_length != INITIAL_BYTES_LENGTH {
            sender.send_error_response(IggyError::CommandLengthError(format!(
                "Unable to read the TCP request length, expected: {INITIAL_BYTES_LENGTH} bytes, received: {read_length} bytes."
            ))).await?;
            continue;
        }

        let length = u32::from_le_bytes(initial_buffer);
        debug!("Received a TCP request, length: {length}");
        let mut command_buffer = BytesMut::with_capacity(length as usize);
        command_buffer.put_bytes(0, length as usize);
        sender.read(&mut command_buffer).await?;
        let command = ServerCommand::from_bytes(command_buffer.freeze());
        if command.is_err() {
            error!("Received an invalid TCP command.");
            sender
                .send_error_response(IggyError::InvalidCommand)
                .await?;
            continue;
        }
        let command = command?;
        if let Err(error) = command.validate() {
            error!("Command validation failed: {error}");
            sender.send_error_response(error).await?;
            continue;
        }

        debug!("Received a TCP command: {command}, payload size: {length}");
        command::handle(command, sender, &session, system.clone()).await?;
    }
}

pub(crate) fn handle_error(error: ServerError) {
    match error {
        ServerError::IoError(error) => match error.kind() {
            ErrorKind::UnexpectedEof => {
                info!("Connection has been closed.");
            }
            ErrorKind::ConnectionAborted => {
                info!("Connection has been aborted.");
            }
            ErrorKind::ConnectionRefused => {
                info!("Connection has been refused.");
            }
            ErrorKind::ConnectionReset => {
                info!("Connection has been reset.");
            }
            _ => {
                error!("Connection has failed: {error}");
            }
        },
        ServerError::SdkError(sdk_error) => match sdk_error {
            IggyError::ConnectionClosed => {
                debug!("Client closed connection.");
            }
            _ => {
                error!("Failure in internal SDK call: {sdk_error}");
            }
        },
        _ => {
            error!("Connection has failed: {error}");
        }
    }
}
