use crate::binary::command;
use crate::binary::sender::Sender;
use crate::server_error::ServerError;
use crate::streaming::clients::client_manager::Transport;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use bytes::{BufMut, Bytes, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::Command;
use iggy::error::IggyError;
use std::io::ErrorKind;
use std::net::SocketAddr;
use monoio::buf::IoBufMut;
use tracing::{debug, error, info};

const INITIAL_BYTES_LENGTH: usize = 4;

pub(crate) async fn handle_connection(
    address: SocketAddr,
    sender: &mut impl Sender,
    system: SharedSystem,
) -> Result<(), ServerError> {
    let client_id = system.read().add_client(&address, Transport::Tcp).await;

    let session = Session::from_client_id(client_id, address);
    loop {
        let initial_buffer = vec![0u8; INITIAL_BYTES_LENGTH];
        let (read_length, mut initial_buffer) = match sender.read(initial_buffer).await {
            (Ok(read_length), buffer) => (read_length, buffer),
            (Err(error), _) => {
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
        let bufer_ptr = initial_buffer.write_ptr();
        let initial_buffer = unsafe { Vec::from_raw_parts(bufer_ptr, INITIAL_BYTES_LENGTH, INITIAL_BYTES_LENGTH) };

        let length = u32::from_le_bytes(initial_buffer.try_into().unwrap());
        debug!("Received a TCP request, length: {length}");
        let mut command_buffer = BytesMut::with_capacity(length as usize);
        command_buffer.put_bytes(0, length as usize);
        let (res, mut command_buffer) = sender.read(command_buffer).await;
        let res_size = res?;
        let command_buffer_ptr = command_buffer.write_ptr();
        let command_buffer = unsafe { Vec::from_raw_parts(command_buffer_ptr, res_size, res_size) };
        let command_buffer = Bytes::from(command_buffer);
        let command = match Command::from_bytes(command_buffer) {
            Ok(command) => command,
            Err(error) => {
                sender.send_error_response(error).await?;
                continue;
            }
        };
        debug!("Received a TCP command: {command}, payload size: {length}");
        command::handle(command, sender, &session, system.clone()).await?;
        debug!("Sent a TCP response.");
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
