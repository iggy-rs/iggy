use crate::binary::client_context::ClientContext;
use crate::binary::mapper;
use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::consumer_type::ConsumerType;
use iggy::error::Error;
use iggy::messages::poll_messages::PollMessages;
use std::sync::Arc;
use streaming::polling_consumer::PollingConsumer;
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
    command: &PollMessages,
    sender: &mut dyn Sender,
    client_context: &ClientContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    if command.count == 0 {
        return Err(Error::InvalidMessagesCount);
    }

    let system = system.read().await;
    let topic = system
        .get_stream(command.stream_id)?
        .get_topic(command.topic_id)?;
    let consumer = match command.consumer_type {
        ConsumerType::Consumer => PollingConsumer::Consumer(command.consumer_id),
        ConsumerType::ConsumerGroup => {
            PollingConsumer::ConsumerGroup(command.consumer_id, client_context.client_id)
        }
    };

    let partition_id = match consumer {
        PollingConsumer::Consumer(_) => command.partition_id,
        PollingConsumer::ConsumerGroup(consumer_group_id, member_id) => {
            let consumer_group = topic.get_consumer_group(consumer_group_id)?.read().await;
            consumer_group.calculate_partition_id(member_id).await?
        }
    };

    let messages = topic
        .get_messages(
            consumer,
            partition_id,
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
    let messages = mapper::map_messages(&messages);
    if command.auto_commit {
        trace!("Last offset: {} will be automatically stored for {}, stream: {}, topic: {}, partition: {}", offset, command.consumer_id, command.stream_id, command.topic_id, command.partition_id);
        topic.store_offset(consumer, partition_id, offset).await?;
    }

    sender.send_ok_response(&messages).await?;
    Ok(())
}
