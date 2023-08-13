use crate::binary::client_context::ClientContext;
use crate::binary::mapper;
use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::error::Error;
use iggy::messages::poll_messages::PollMessages;
use std::sync::Arc;
use streaming::polling_consumer::PollingConsumer;
use streaming::systems::system::System;
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
    let consumer = PollingConsumer::from_consumer(&command.consumer, client_context.client_id);
    let system = system.read().await;
    let messages = system
        .poll_messages(
            &command.stream_id,
            &command.topic_id,
            consumer,
            command.partition_id,
            command.strategy,
            command.count,
            command.auto_commit,
        )
        .await?;
    let messages = mapper::map_messages(&messages);
    sender.send_ok_response(&messages).await?;
    Ok(())
}
