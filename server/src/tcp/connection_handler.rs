use crate::binary::sender::Sender;
use crate::server_error::ServerError;
use crate::streaming::clients::client_manager::Transport;
use crate::streaming::session::Session;
use crate::tpc::shard::shard_frame::{ShardResponse};
use crate::{tpc::shard::shard::IggyShard};
use bytes::{BufMut, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::{Command, CommandExecution, CommandExecutionOrigin};
use iggy::error::IggyError;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::rc::Rc;
use tracing::{debug, error, info};

const INITIAL_BYTES_LENGTH: usize = 4;

pub(crate) async fn handle_connection(
    address: SocketAddr,
    sender: &mut impl Sender,
    shard: Rc<IggyShard>,
) -> Result<(), ServerError> {
    let client_id = shard.add_client(&address, Transport::Tcp).await;
    let session = Session::from_client_id(client_id, address);
    loop {
        let initial_buffer = BytesMut::with_capacity(INITIAL_BYTES_LENGTH);
        let (read_length, initial_buffer) = match sender.read(initial_buffer).await {
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

        let initial_buffer = initial_buffer.freeze();
        let length = u32::from_le_bytes(initial_buffer[0..4].try_into().unwrap());
        debug!("Received a TCP request, length: {length}");
        let mut command_buffer = BytesMut::with_capacity(length as usize);
        command_buffer.put_bytes(0, length as usize);
        let (res, command_buffer) = sender.read(command_buffer).await;
        let _ = res?;
        let command_buffer = command_buffer.freeze();
        // We've got the command now we need to route it to appropiate thread.
        let command = match Command::from_bytes(command_buffer) {
            Ok(command) => command,
            Err(error) => {
                sender.send_error_response(error).await?;
                continue;
            }
        };
        // We have to validate the session before we dispatch the request.

        match command.get_command_execution_origin() {
            CommandExecution::Direct => {

            },
            CommandExecution::Routed(cmd_hash) => {
                match shard.send_request_to_shard(cmd_hash, command).await? {
                    ShardResponse::BinaryResponse(payload) => {
                        sender.send_ok_response(&payload).await?;
                    },
                    ShardResponse::ErrorResponse(err) => {
                        sender.send_error_response(err).await?;
                    }
                }
            }
        }

        /*
        debug!("Received a TCP command: {command}, payload size: {length}");
        debug!("Sent a TCP response.");
        */
        //command::handle(command, sender, &session, system.clone()).await?;
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
