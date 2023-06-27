use crate::binary::handlers::groups::{create_group_handler, delete_group_handler};
use crate::binary::handlers::messages::*;
use crate::binary::handlers::offsets::*;
use crate::binary::handlers::streams::*;
use crate::binary::handlers::system::*;
use crate::binary::handlers::topics::*;
use crate::binary::sender::Sender;
use sdk::command::Command;
use sdk::error::Error;
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
        Command::Kill(command) => kill_handler::handle(command, sender).await,
        Command::Ping(command) => ping_handler::handle(command, sender).await,
        Command::GetClients(command) => get_clients_handler::handle(command, sender, system).await,
        Command::SendMessages(command) => {
            send_messages_handler::handle(command, sender, system).await
        }
        Command::PollMessages(command) => {
            poll_messages_handler::handle(command, sender, system).await
        }
        Command::GetOffset(command) => get_offset_handler::handle(command, sender, system).await,
        Command::StoreOffset(command) => {
            store_offset_handler::handle(command, sender, system).await
        }
        Command::GetStream(command) => get_stream_handler::handle(command, sender, system).await,
        Command::GetStreams(command) => get_streams_handler::handle(command, sender, system).await,
        Command::CreateStream(command) => {
            create_stream_handler::handle(command, sender, system).await
        }
        Command::DeleteStream(command) => {
            delete_stream_handler::handle(command, sender, system).await
        }
        Command::GetTopic(command) => get_topic_handler::handle(command, sender, system).await,
        Command::GetTopics(command) => get_topics_handler::handle(command, sender, system).await,
        Command::CreateTopic(command) => {
            create_topic_handler::handle(command, sender, system).await
        }
        Command::DeleteTopic(command) => {
            delete_topic_handler::handle(command, sender, system).await
        }
        Command::CreateGroup(command) => {
            create_group_handler::handle(command, sender, system).await
        }
        Command::DeleteGroup(command) => {
            delete_group_handler::handle(command, sender, system).await
        }
    }
}
