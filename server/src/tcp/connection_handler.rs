use crate::binary::command;
use crate::binary::sender::Sender;
use crate::command::ServerCommand;
use crate::server_error::ConnectionError;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use bytes::{BufMut, BytesMut};
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::Partitioning;
use iggy::validatable::Validatable;
use iggy::{bytes_serializable::BytesSerializable, command::SEND_MESSAGES_CODE};
use rkyv::util::AlignedVec;
use std::io::ErrorKind;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

const INITIAL_BYTES_LENGTH: usize = 4;

pub(crate) async fn handle_connection(
    session: Arc<Session>,
    sender: &mut dyn Sender,
    system: SharedSystem,
) -> Result<(), ConnectionError> {
    let mut initial_buffer = [0u8; INITIAL_BYTES_LENGTH];
    let mut code_buffer = [0u8; INITIAL_BYTES_LENGTH];
    loop {
        let read_length: usize = match sender.read(&mut initial_buffer).await {
            Ok(read_length) => read_length,
            Err(error) => {
                if error.as_code() == IggyError::ConnectionClosed.as_code() {
                    return Err(ConnectionError::from(error));
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
        let length = u32::from_le_bytes(initial_buffer) - 4;
        sender.read(&mut code_buffer).await?;
        let code = u32::from_le_bytes(code_buffer);
        if code == SEND_MESSAGES_CODE {
            let mut metadata_len_buf = [0u8; 8];
            sender.read(&mut metadata_len_buf).await?;
            let metadata_len = u64::from_le_bytes(metadata_len_buf.try_into().unwrap());
            let mut metadata_buf = vec![0u8; metadata_len as _];
            sender.read(&mut metadata_buf).await?;
            let mut position = 0;
            let (read, partitioning) =
                Partitioning::from_bytes_new(&metadata_buf[position..]).unwrap();
            position += read;
            let (read, stream_id) = Identifier::from_bytes_new(&metadata_buf[position..]).unwrap();
            position += read;
            let (_, topic_id) = Identifier::from_bytes_new(&metadata_buf[position..]).unwrap();

            let messages_len = length - metadata_len as u32 - 8;
            let mut batch = AlignedVec::with_capacity(messages_len as _);
            unsafe {
                batch.set_len(messages_len as _);
            }
            sender.read(&mut batch).await?;

            {
                system
                    .read()
                    .await
                    .append_messages(&session, stream_id, topic_id, partitioning, batch, None)
                    .await?;
            }
            sender.send_empty_ok_response().await?;
        } else {
            debug!("Received a TCP request, length: {length}");
            let mut command_buffer = BytesMut::with_capacity(length as usize);
            command_buffer.put_bytes(0, length as usize);
            if length > 0 {
                sender.read(&mut command_buffer).await?;
            }
            let command_buffer = command_buffer.freeze();
            let command = ServerCommand::from_bytes(code, command_buffer);
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
}

pub(crate) fn handle_error(error: ConnectionError) {
    match error {
        ConnectionError::IoError(error) => match error.kind() {
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
        ConnectionError::SdkError(sdk_error) => match sdk_error {
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
