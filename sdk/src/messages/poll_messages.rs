use crate::client::Client;
use crate::error::Error;
use crate::message::Message;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::messages::poll_messages::PollMessages;

impl Client {
    pub async fn poll_messages(&mut self, command: &PollMessages) -> Result<Vec<Message>, Error> {
        let response = self
            .send_with_response(
                [Command::PollMessages.as_bytes(), command.as_bytes()]
                    .concat()
                    .as_slice(),
            )
            .await?;
        handle_response(response)
    }
}

fn handle_response(response: &[u8]) -> Result<Vec<Message>, Error> {
    if response.is_empty() {
        return Ok(Vec::new());
    }

    let length = response.len();
    let mut position = 4;
    let mut messages = Vec::new();
    while position < length {
        let offset = u64::from_le_bytes(response[position..position + 8].try_into()?);
        let timestamp = u64::from_le_bytes(response[position + 8..position + 16].try_into()?);
        let message_length =
            u64::from_le_bytes(response[position + 16..position + 24].try_into()?) as usize;
        let payload = response[position + 24..position + 24 + message_length].to_vec();

        position = position + 24 + message_length;
        messages.push(Message {
            offset,
            timestamp,
            length: message_length as u64,
            payload,
        });
        if position >= length {
            break;
        }
    }

    messages.sort_by(|x, y| x.offset.cmp(&y.offset));
    Ok(messages)
}
