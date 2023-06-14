use crate::binary::sender::Sender;
use crate::utils::binary_mapper;
use anyhow::Result;
use shared::error::Error;
use shared::messages::poll_messages::PollMessages;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

/*
    |  POLL   | CONSUMER  |   STREAM  |   TOPIC   |    PT_ID  |    KIND   |   VALUE   |   COUNT   |   COMMIT  |
    | 1 byte  |  4 bytes  |  4 bytes  |  4 bytes  |   4 bytes |   1 byte  |  8 bytes  |  4 bytes  |   1 byte  |

    POLL
        - Constant 1 byte of value 2

    CONSUMER:
        - Arbitrary Consumer ID that might be used for offset tracking.

    STREAM:
        - Unique Stream ID to poll the messages from.

    TOPIC:
        - Unique Topic ID to poll the messages from.

    KIND:
        - 0 -> offset
        - 1 -> timestamp
        - 2 -> first
        - 3 -> last
        - 4 -> next

    VALUE:
        - when KIND is 0, value is the exact offset
        - when KIND is 1, value is the timestamp (greater than or equal to)
        - when KIND is 2, 3, 4, value is ignored

    COUNT:
        - Number of messages to poll in a single chunk.

    COMMIT:
        - Auto commit flag, if true, the consumer offset will be stored automatically.

    Poll the message(s) by consumer: 0, stream: 1, topic: 1, partition: 1, using kind: offset, value is 0, messages count is 1.
    |    0    |    1    |     1     |     1     |     0     |     0     |     1     |

    Poll the message(s) by consumer: 0, stream: 1, topic: 1, partition: 1, using kind: timestamp, value is 1679997285, messages count is 1.
    |    0    |    1    |     1     |     1     |     1     | 1679997285|     1     |

    Poll the message(s) by consumer: 0, stream: 1, topic: 1, partition: 1, using kind: first, value is ignored, messages count is 1.
    |    0    |    1    |     1     |     1     |     2     |     0     |     1     |

    Poll the message(s) by consumer: 0, stream: 1, topic: 1, partition: 1, using kind: last, value is ignored, messages count is 1.
    |    0    |    1    |     1     |     1     |     3     |     0     |     1     |

    Poll the message(s) by consumer: 0, stream: 1, topic: 1, partition: 1, using kind: next, value is ignored, messages count is 1.
    |    0    |    1    |     1     |     1     |     4     |     0     |     1     |
*/

pub async fn handle(
    command: PollMessages,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    if command.count == 0 {
        return Err(Error::InvalidMessagesCount);
    }

    let system = system.read().await;
    let stream = system.get_stream(command.stream_id)?;
    let messages = stream
        .get_messages(
            command.consumer_id,
            command.topic_id,
            command.partition_id,
            command.kind,
            command.value,
            command.count,
        )
        .await?;

    if messages.is_empty() {
        sender.send_empty_ok_response().await?;
        return Ok(());
    }

    let offset = messages.last().unwrap().offset;
    let messages = binary_mapper::map_messages(messages);
    if command.auto_commit {
        trace!("Last offset: {} will be automatically stored for consumer: {}, stream: {}, topic: {}, partition: {}", offset, command.consumer_id, command.stream_id, command.topic_id, command.partition_id);
        stream
            .store_offset(
                command.consumer_id,
                command.topic_id,
                command.partition_id,
                offset,
            )
            .await?;
    }

    sender.send_ok_response(&messages).await?;
    Ok(())
}
