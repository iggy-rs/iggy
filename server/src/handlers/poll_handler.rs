use crate::handlers::STATUS_OK;
use anyhow::Result;
use std::net::SocketAddr;
use streaming::error::Error;
use streaming::system::System;
use tokio::net::UdpSocket;
use tracing::info;

pub const COMMAND: &[u8] = &[2];

/*
    |  POLL   |   STREAM  |   TOPIC   |    PT_ID  |    KIND   |   VALUE   |   COUNT   |
    | 1 byte  |  4 bytes  |  4 bytes  |   4 bytes |   1 byte  |  8 bytes  |  4 bytes  |

    POLL
        - Constant 1 byte of value 2

    STREAM:
        - Unique Stream ID to poll the messages from.

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

    Poll the message(s) from stream: 1, topic: 1, partition: 1, using kind: first, value is ignored, messages count is 1.
    |    2    |     1     |     1     |     1     |     0     |     0     |     1     |

    Poll the message(s) stream: 1, topic: 1, partition: 1, using kind: last, value is ignored, messages count is 1.
    |    2    |     1     |     1     |     1     |     1     |     0     |     1     |

    Poll the message(s) stream: 1, topic: 1, partition: 1, using kind: next, value is ignored, messages count is 1.
    |    2    |     1     |     1     |     1     |     2     |     0     |     1     |

    Poll the message(s) stream: 1, topic: 1, partition: 1, using kind: offset, value is 1, messages count is 1.
    |    2    |     1     |     1     |     1     |     3     |     1     |     1     |

    Poll the message(s) stream: 1, topic: 1, partition: 1, using kind: timestamp, value is 1679997285, messages count is 1.
    |    2    |     1     |     1     |     1     |     4     |1679997285 |     1     |
*/

const LENGTH: usize = 25;

pub async fn handle(
    input: &[u8],
    socket: &UdpSocket,
    address: SocketAddr,
    system: &mut System,
) -> Result<(), Error> {
    if input.len() != LENGTH {
        return Err(Error::InvalidCommand);
    }

    let stream = u32::from_le_bytes(input[..4].try_into().unwrap());
    let topic = u32::from_le_bytes(input[4..8].try_into().unwrap());
    let partition_id = u32::from_le_bytes(input[8..12].try_into().unwrap());
    let kind = input[12];
    let value = u64::from_le_bytes(input[13..21].try_into().unwrap());
    let count = u32::from_le_bytes(input[21..25].try_into().unwrap());
    if count == 0 {
        return Err(Error::InvalidMessagesCount);
    }

    info!(
        "Polling messages from stream: {:?}, topic: {:?}, kind: {:?}, value: {:?}, count: {:?}",
        stream, topic, kind, value, count
    );

    let messages = system
        .get_stream(stream)?
        .get_messages(topic, partition_id, value, count)?;
    let messages_count = messages.len() as u32;
    let data = messages
        .iter()
        .map(|message| {
            [
                message.offset.to_le_bytes().as_slice(),
                message.timestamp.to_le_bytes().as_slice(),
                message.length.to_le_bytes().as_slice(),
                message.payload.as_slice(),
            ]
            .concat()
        })
        .collect::<Vec<Vec<u8>>>()
        .concat();

    socket
        .send_to(
            [
                STATUS_OK,
                messages_count.to_le_bytes().as_slice(),
                data.as_slice(),
            ]
            .concat()
            .as_slice(),
            address,
        )
        .await?;
    info!(
        "Polled {} message(s) from stream, topic: {:?}, kind: {:?}, value: {:?}, count: {:?}",
        messages_count, topic, kind, value, count
    );
    Ok(())
}
