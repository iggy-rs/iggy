use anyhow::Result;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tracing::info;
use streaming::stream::Stream;
use streaming::stream_error::StreamError;
use crate::handlers::STATUS_OK;

pub const COMMAND: &[u8] = &[2];

/*
    |  POLL   |   TOPIC   |    KIND   |   VALUE   |   COUNT   |
    | 1 byte  |  4 bytes  |   1 byte  |  8 bytes  |  4 bytes  |

    POLL
        - Constant 1 byte of value 2

    TOPIC:
        - Unique Topic ID to poll the messages from.

    KIND:
        - 0 -> first
        - 1 -> last
        - 2 -> next
        - 3 -> offset
        - 4 -> timestamp

    VALUE:
        - when KIND is 0, 1, 2, value is ignored
        - when KIND is 3, value is the exact offset
        - when KIND is 4, value is the timestamp (greater than or equal to)

    COUNT:
        - Number of messages to poll in a single chunk.

    Poll the message(s) from topic: 1, using kind: first, value is ignored, messages count is 1.
    |    2    |     1     |     0     |     0     |     1     |

    Poll the message(s) from topic: 1, using kind: last, value is ignored, messages count is 1.
    |    2    |     1     |     1     |     0     |     1     |

    Poll the message(s) from topic: 1, using kind: next, value is ignored, messages count is 1.
    |    2    |     1     |     2     |     0     |     1     |

    Poll the message(s) from topic: 1, using kind: offset, value is 1, messages count is 1.
    |    2    |     1     |     3     |     1     |     1     |

    Poll the message(s) from topic: 1, using kind: timestamp, value is 1679997285, messages count is 1.
    |    2    |     1     |     4     |1679997285 |     1     |
*/

const LENGTH: usize = 21;

pub async fn handle(input: &[u8], socket: &UdpSocket, address: SocketAddr, stream: &mut Stream) -> Result<(), StreamError> {
    if input.len() != LENGTH {
        return Err(StreamError::InvalidCommand);
    }

    let topic = u32::from_le_bytes(input[..4].try_into().unwrap());
    let partition_id = u32::from_le_bytes(input[4..8].try_into().unwrap());
    let kind = input[8];
    let value = u64::from_le_bytes(input[9..17].try_into().unwrap());
    let count = u32::from_le_bytes(input[17..21].try_into().unwrap());
    if count == 0 {
        return Err(StreamError::InvalidMessagesCount);
    }

    info!("Polling from stream, topic: {:?}, kind: {:?}, value: {:?}, count: {:?}", topic, kind, value, count);

    let messages = stream.get_messages(topic, partition_id, value, count)?;
    let messages_count = messages.len() as u32;
    let data = messages.iter().map(|message| [
        message.offset.to_le_bytes().as_slice(),
        message.timestamp.to_le_bytes().as_slice(),
        (message.body.len() as u64).to_le_bytes().as_slice(),
        message.body.as_slice()].concat())
        .collect::<Vec<Vec<u8>>>()
        .concat();

    socket.send_to([STATUS_OK, messages_count.to_le_bytes().as_slice(), data.as_slice()].concat().as_slice(), address).await?;
    info!("Polled {} message(s) from stream, topic: {:?}, kind: {:?}, value: {:?}, count: {:?}, messages: {:?}", messages_count, topic, kind, value, count, messages);
    Ok(())
}