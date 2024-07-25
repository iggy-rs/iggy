use crate::binary::sender::Sender;
use crate::command::ServerCommand;
use crate::handle_response;
use crate::server_error::ServerError;
use crate::streaming::clients::client_manager::Transport;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::session::Session;
use crate::tpc::shard::shard::IggyShard;
use crate::tpc::shard::shard_frame::{ShardEvent, ShardMessage, ShardResponse};
use bytes::{BufMut, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::identifier::IdKind;
use iggy::messages::send_messages::PartitioningKind;
use iggy::models::resource_namespace::IggyResourceNamespace;
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
    let event = ShardEvent::NewSession(client_id, address, Transport::Tcp);
    let response = shard.broadcast_event_to_all_shards(client_id, event);
    for resp in response {
        resp?;
    }
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
        let mut command = match ServerCommand::from_bytes(command_buffer) {
            Ok(command) => command,
            Err(error) => {
                sender.send_error_response(error).await?;
                continue;
            }
        };

        // This is really ugly, better solution would be to decouple stream and topic `Identifier` structs
        // from the `Stream` and `Topic` structs
        // into it's own tables and store them on top level shard.

        // Generally it's good idea to use only lightweight versions of `Stream`, `Topic`, `Partition`,
        // maybe even `Segment` structs, and store all the data in the `IggyShard` struct
        // as some sort of map, maybe some of those could be even shared, with the usage of
        // https://github.com/ibraheemdev/papaya

        // Long-term decouple those even further into corresponding `frontends` and `backends`.
        let response = match command {
            ServerCommand::SendMessages(ref cmd) => {
                let stream_id = if let IdKind::Numeric = cmd.stream_id.kind {
                    cmd.stream_id
                        .get_u32_value()
                        .expect("Unable to get numeric stream id, despite it being numeric.")
                } else {
                    // lookup in the streams hashmap
                    let stringify_stream_id = cmd
                        .stream_id
                        .get_string_value()
                        .expect("Failed to get string value out of stream identifier");
                    *shard
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
                    let stringify_topic_id = cmd
                        .topic_id
                        .get_string_value()
                        .expect("Failed to get string value out of topic identifier");
                    *shard
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
                        let streams = shard.streams.borrow();
                        let topic = streams
                            .get(&stream_id)
                            .expect("Stream not found")
                            .topics
                            .get(&topic_id)
                            .expect("Topic not found");
                        topic.get_next_partition_id()
                    }
                    PartitioningKind::PartitionId => u32::from_le_bytes(
                        cmd.partitioning.value[..cmd.partitioning.length as usize].try_into()?,
                    ),
                    PartitioningKind::MessagesKey => {
                        let streams = shard.streams.borrow();
                        let topic = streams
                            .get(&stream_id)
                            .expect("Stream not found")
                            .topics
                            .get(&topic_id)
                            .expect("Topic not found");
                        topic.calculate_partition_id_by_messages_key_hash(&cmd.partitioning.value)
                    }
                };
                let resource_ns = IggyResourceNamespace::new(stream_id, topic_id, partition_id);
                let response = shard
                    .send_request_to_shard(client_id, resource_ns, command)
                    .await?;
                response
            }
            ServerCommand::PollMessages(ref mut cmd) => {
                let stream_id = if let IdKind::Numeric = cmd.stream_id.kind {
                    cmd.stream_id
                        .get_u32_value()
                        .expect("Unable to get numeric stream id, despite it being numeric.")
                } else {
                    let stringify_stream_id = cmd
                        .stream_id
                        .get_string_value()
                        .expect("Failed to get string value out of stream identifier");
                    *shard
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
                    let stringify_topic_id = cmd
                        .topic_id
                        .get_string_value()
                        .expect("Failed to get string value out of topic identifier");
                    *shard
                        .streams
                        .borrow()
                        .get(&stream_id)
                        .expect("Stream not found")
                        .topics_ids
                        .get(&stringify_topic_id)
                        .expect("Topic not found")
                };
                let consumer =
                    PollingConsumer::from_consumer(&cmd.consumer, client_id, &cmd.partition_id);
                let partition_id = match consumer {
                    PollingConsumer::Consumer(_, partition_id) => partition_id,
                    PollingConsumer::ConsumerGroup(group_id, member_id) => {
                        let topic = shard
                            .find_topic(client_id, &cmd.stream_id, &cmd.topic_id)
                            .expect("Failed to find topic");
                        let consumer_group = topic.get_consumer_group_by_id(group_id)?;
                        consumer_group.calculate_partition_id(member_id).await?
                    }
                };
                cmd.partition_id = Some(partition_id);
                let resource_ns = IggyResourceNamespace::new(stream_id, topic_id, partition_id);
                let response = shard
                    .send_request_to_shard(client_id, resource_ns, command)
                    .await?;
                response
            }
            _ => {
                let message = ShardMessage::Command(command);
                error!("Received a command: {:?}", message);
                shard
                    .handle_shard_message(client_id, message)
                    .await
                    .expect("Failed to handle a shard command for direct request execution, it should always return a response.")
            }
        };
        error!("Response: {:?}", response);
        handle_response!(sender, response);

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
