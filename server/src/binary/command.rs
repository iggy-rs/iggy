use crate::binary::handlers::messages::*;
use crate::binary::handlers::offsets::*;
use crate::binary::handlers::streams::*;
use crate::binary::handlers::system::*;
use crate::binary::handlers::topics::*;
use crate::binary::sender::Sender;
use shared::command::Command;
use shared::error::Error;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

/*
  FRAME: | COMMAND |   DATA    |
         | 1 byte  |  n bytes  |

1. PING: | COMMAND |
         | 1 byte  |

2. POLL: | COMMAND |   STREAM  |   TOPIC   |    KIND   |   VALUE   |   COUNT   |
         | 1 byte  |  4 bytes  |  4 bytes  |   1 byte  |  8 bytes  |  4 bytes  |

3. SEND: | COMMAND |   STREAM  |    TOPIC   |    KIND   |   VALUE   |   COUNT   |  PAYLOAD  |
         | 1 byte  |  4 bytes  |   4 bytes  |   1 byte  |  8 bytes  |  4 bytes  |  n bytes  |
*/

/*
  RESPONSE: |   STATUS  |   DATA    |
            |   1 byte  |  n bytes  |
*/

pub async fn handle(
    command: Command,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    let result = try_handle(command, sender, system).await;
    if result.is_ok() {
        trace!("Command was handled successfully.");
        return Ok(());
    }

    let error = result.err().unwrap();
    trace!(
        "Command was not handled successfully, error: '{:?}'.",
        error
    );
    sender.send_error_response(error).await?;
    Ok(())
}

async fn try_handle(
    command: Command,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("Handling command '{}'...", command);
    match command {
        Command::Kill(payload) => kill_handler::handle(payload, sender).await,
        Command::Ping(payload) => ping_handler::handle(payload, sender).await,
        Command::SendMessages(payload) => {
            send_messages_handler::handle(payload, sender, system).await
        }
        Command::PollMessages(payload) => {
            poll_messages_handler::handle(payload, sender, system).await
        }
        Command::StoreOffset(payload) => {
            store_offset_handler::handle(payload, sender, system).await
        }
        Command::GetStreams(payload) => get_streams_handler::handle(payload, sender, system).await,
        Command::CreateStream(payload) => {
            create_stream_handler::handle(payload, sender, system).await
        }
        Command::DeleteStream(payload) => {
            delete_stream_handler::handle(payload, sender, system).await
        }
        Command::GetTopics(payload) => get_topics_handler::handle(payload, sender, system).await,
        Command::CreateTopic(payload) => {
            create_topic_handler::handle(payload, sender, system).await
        }
        Command::DeleteTopic(payload) => {
            delete_topic_handler::handle(payload, sender, system).await
        }
    }
}
