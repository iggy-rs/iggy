use crate::handlers::messages::*;
use crate::handlers::offsets::store_offset_handler;
use crate::handlers::streams::*;
use crate::handlers::system::*;
use crate::handlers::topics::*;
use crate::sender::Sender;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::error::Error;
use shared::messages::poll_messages::PollMessages;
use shared::messages::send_messages::SendMessages;
use shared::offsets::store_offset::StoreOffset;
use shared::streams::create_stream::CreateStream;
use shared::streams::delete_stream::DeleteStream;
use shared::topics::create_topic::CreateTopic;
use shared::topics::delete_topic::DeleteTopic;
use shared::topics::get_topics::GetTopics;
use streaming::system::System;
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

const LENGTH: usize = 1;

pub async fn handle(request: &[u8], sender: &mut Sender, system: &mut System) -> Result<(), Error> {
    if request.len() < LENGTH {
        return Err(Error::InvalidCommand);
    }

    trace!("Trying to read command...");
    let command = Command::from_bytes(&request[..LENGTH])?;
    let bytes = &request[LENGTH..];
    trace!(
        "Received command: '{:?}', payload size: {}, trying to handle...",
        command,
        bytes.len()
    );
    let result = try_handle(command, bytes, sender, system).await;
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
    bytes: &[u8],
    sender: &mut Sender,
    system: &mut System,
) -> Result<(), Error> {
    trace!("Handling command '{}'...", command);
    match command {
        Command::Ping => ping_handler::handle(sender).await,
        Command::SendMessages => {
            let command = SendMessages::from_bytes(bytes)?;
            send_messages_handler::handle(command, sender, system).await
        }
        Command::PollMessages => {
            let command = PollMessages::from_bytes(bytes)?;
            poll_messages_handler::handle(command, sender, system).await
        }
        Command::StoreOffset => {
            let command = StoreOffset::from_bytes(bytes)?;
            store_offset_handler::handle(command, sender, system).await
        }
        Command::GetStreams => get_streams_handler::handle(sender, system).await,
        Command::CreateStream => {
            let command = CreateStream::from_bytes(bytes)?;
            create_stream_handler::handle(command, sender, system).await
        }
        Command::DeleteStream => {
            let command = DeleteStream::from_bytes(bytes)?;
            delete_stream_handler::handle(command, sender, system).await
        }
        Command::GetTopics => {
            let command = GetTopics::from_bytes(bytes)?;
            get_topics_handler::handle(command, sender, system).await
        }
        Command::CreateTopic => {
            let command = CreateTopic::from_bytes(bytes)?;
            create_topic_handler::handle(command, sender, system).await
        }
        Command::DeleteTopic => {
            let command = DeleteTopic::from_bytes(bytes)?;
            delete_topic_handler::handle(command, sender, system).await
        }
    }
}
