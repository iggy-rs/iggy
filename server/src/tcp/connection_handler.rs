use crate::binary::sender::Sender;
use crate::handle_response;
use crate::server_error::ServerError;
use crate::streaming::clients::client_manager::Transport;
use crate::streaming::session::Session;
use crate::tpc::shard::cmd_handler;
use crate::tpc::shard::shard::IggyShard;
use crate::tpc::shard::shard_frame::{ShardEvent, ShardMessage, ShardResponse};
use bytes::{BufMut, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::{Command, CommandExecution, CommandExecutionOrigin};
use iggy::error::IggyError;
use iggy::messages::send_messages::PartitioningKind;
use iggy::models::messages::POLLED_MESSAGE_METADATA;
use iggy::models::resource_namespace;
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
    shard.add_active_session(session);
    // Broadcast session to all shards.
    let event = ShardEvent::NewSession(client_id, address);
    shard.broadcast_event_to_all_shards(client_id, event);
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

        match command {
            Command::SendMessages(cmd) => {
                // This is really ugly, better solution would be to decouple stream and topic ids
                // from the `Stream` and `Topic` structs
                // into it's own tables (vectors) and store them on top level of the shard.
                let stream_id = if let IdKind::Numeric = cmd.stream_id.kind {
                    cmd.stream_id
                        .get_u32_value()
                        .expect("Unable to get numeric stream id, despite it being numeric.")
                } else {
                    // lookup in the streams hashmap
                    let stringify_stream_id = cmd.stream_id.get_string_value();
                    *self
                        .streams_ids
                        .borrow()
                        .get(&stringify_stream_id)
                        .expect("Stream not found")
                };
                let topic_id = if let IdKind::Numeric = cmd.topic_id.kind {
                    cmd.topic_id
                        .get_u32_value()
                        .expect("Unable to get numeric topic id, despite it being numeric.")
                } else {
                    // lookup in the streams hashmap
                    let stringify_topic_id = cmd.topic_id.get_string_value();
                    *self
                        .streams
                        .borrow()
                        .get(&stream_id)
                        .expect("Stream not found")
                        .topics_ids
                        .get(&stringify_topic_id)
                        .expect("Topic not found")
                };
                let partition_id = match cmd.partitioning.kind {
                    PartitioningKind::Balanced => {
                        let topic = shard
                            .streams
                            .borrow()
                            .get(&stream_id)
                            .expect("Stream not found")
                            .topics
                            .get(&topic_id)
                            .expect("Topic not found");
                        topic.get_next_partition_id()
                    }
                    PartitioningKind::PartitionId => {
                        u32::from_le_bytes(partitioning.value[..partitioning.length as usize].try_into()?)
                    }
                    PartitioningKind::MessagesKey => {
                        let topic = shard
                            .streams
                            .borrow()
                            .get(&stream_id)
                            .expect("Stream not found")
                            .topics
                            .get(&topic_id)
                            .expect("Topic not found");
                        topic.calculate_partition_id_by_messages_key_hash(&cmd.partitioning.value)
                    }
                };
                let resource_ns = IggyResourceNamespace::new(stream_id, topic_id, partition_id);
                let response = self.send_request_to_shard(client_id, resource_ns, command);

                continue;
            }
            Command::PollMessages(cmd) => {}
            _ => None,
        }

        /* 
        match command.get_command_execution_origin() {
            CommandExecution::Direct => {
                let message = ShardMessage::Command(command);
                let response = shard
                    .handle_shard_message(client_id, message)
                    .await
                    .expect("Failed to handle a shard command for direct request execution, it should always return a response.");
                handle_response!(sender, response);
            }
            CommandExecution::Routed() => {
                let response = shard
                    .send_request_to_shard(client_id, resource_ns, command)
                    .await?;
                handle_response!(sender, response);
            }
        }
        */

        /*
        debug!("Received a TCP command: {command}, payload size: {length}");
        debug!("Sent a TCP response.");
        */
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
