use crate::binary::binary_client::BinaryClient;
use crate::error::Error;
use crate::message::Message;
use shared::command::Command;
use shared::messages::poll_messages::PollMessages;
use shared::messages::send_messages::SendMessages;
use shared::offsets::store_offset::StoreOffset;

pub async fn poll_messages(
    client: &dyn BinaryClient,
    command: PollMessages,
) -> Result<Vec<Message>, Error> {
    let response = client
        .send_with_response(Command::PollMessages(command))
        .await?;
    handle_poll_messages_response(&response)
}

pub async fn send_messages(client: &dyn BinaryClient, command: SendMessages) -> Result<(), Error> {
    client
        .send_with_response(Command::SendMessages(command))
        .await?;
    Ok(())
}

pub async fn store_offset(client: &dyn BinaryClient, command: StoreOffset) -> Result<(), Error> {
    client
        .send_with_response(Command::StoreOffset(command))
        .await?;
    Ok(())
}

fn handle_poll_messages_response(response: &[u8]) -> Result<Vec<Message>, Error> {
    if response.is_empty() {
        return Ok(Vec::new());
    }

    const PROPERTIES_SIZE: usize = 36;
    let length = response.len();
    let mut position = 4;
    let mut messages = Vec::new();
    while position < length {
        let offset = u64::from_le_bytes(response[position..position + 8].try_into()?);
        let timestamp = u64::from_le_bytes(response[position + 8..position + 16].try_into()?);
        let id = u128::from_le_bytes(response[position + 16..position + 32].try_into()?);
        let message_length =
            u32::from_le_bytes(response[position + 32..position + PROPERTIES_SIZE].try_into()?);

        let payload_range =
            position + PROPERTIES_SIZE..position + PROPERTIES_SIZE + message_length as usize;
        if payload_range.start > length || payload_range.end > length {
            break;
        }

        let payload = response[payload_range].to_vec();
        let total_size = PROPERTIES_SIZE + message_length as usize;
        position += total_size;
        messages.push(Message {
            offset,
            timestamp,
            id,
            length: message_length,
            payload,
        });

        if position + PROPERTIES_SIZE >= length {
            break;
        }
    }

    messages.sort_by(|x, y| x.offset.cmp(&y.offset));
    Ok(messages)
}
