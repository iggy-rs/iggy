use crate::binary::client_context::ClientContext;
use crate::binary::handlers::consumer_groups::{
    create_consumer_group_handler, delete_consumer_group_handler, get_consumer_group_handler,
    get_consumer_groups_handler, join_consumer_group_handler, leave_consumer_group_handler,
};
use crate::binary::handlers::messages::*;
use crate::binary::handlers::offsets::*;
use crate::binary::handlers::streams::*;
use crate::binary::handlers::system::*;
use crate::binary::handlers::topics::*;
use crate::binary::sender::Sender;
use iggy::command::Command;
use iggy::error::Error;
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
    command: &Command,
    sender: &mut dyn Sender,
    client_context: &ClientContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    let result = try_handle(command, sender, client_context, system).await;
    if result.is_ok() {
        trace!(
            "Command was handled successfully, client: '{}'.",
            client_context
        );
        return Ok(());
    }

    let error = result.err().unwrap();
    trace!(
        "Command was not handled successfully, client: {}, error: '{:?}'.",
        client_context,
        error
    );
    sender.send_error_response(error).await?;
    Ok(())
}

async fn try_handle(
    command: &Command,
    sender: &mut dyn Sender,
    client_context: &ClientContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!(
        "Handling command '{}', client: {}...",
        command,
        client_context
    );
    match command {
        Command::Kill(command) => kill_handler::handle(command, sender).await,
        Command::Ping(command) => ping_handler::handle(command, sender).await,
        Command::GetStats(command) => get_stats_handler::handle(command, sender, system).await,
        Command::GetMe(command) => {
            get_me_handler::handle(command, sender, client_context, system).await
        }
        Command::GetClient(command) => get_client_handler::handle(command, sender, system).await,
        Command::GetClients(command) => get_clients_handler::handle(command, sender, system).await,
        Command::SendMessages(command) => {
            send_messages_handler::handle(command, sender, system).await
        }
        Command::PollMessages(command) => {
            poll_messages_handler::handle(command, sender, client_context, system).await
        }
        Command::GetOffset(command) => {
            get_offset_handler::handle(command, sender, client_context, system).await
        }
        Command::StoreOffset(command) => {
            store_offset_handler::handle(command, sender, client_context, system).await
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
        Command::GetGroup(command) => {
            get_consumer_group_handler::handle(command, sender, system).await
        }
        Command::GetGroups(command) => {
            get_consumer_groups_handler::handle(command, sender, system).await
        }
        Command::CreateGroup(command) => {
            create_consumer_group_handler::handle(command, sender, system).await
        }
        Command::DeleteGroup(command) => {
            delete_consumer_group_handler::handle(command, sender, system).await
        }
        Command::JoinGroup(command) => {
            join_consumer_group_handler::handle(command, sender, client_context, system).await
        }
        Command::LeaveGroup(command) => {
            leave_consumer_group_handler::handle(command, sender, client_context, system).await
        }
    }
}
