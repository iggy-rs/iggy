use crate::client::Client;
use crate::error::Error;
use crate::message::Message;

const COMMAND: &[u8] = &[2];

impl Client {
    pub async fn poll_messages(
        &mut self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        kind: u8,
        value: u64,
        count: u32,
    ) -> Result<Vec<Message>, Error> {
        let stream_id = &stream_id.to_le_bytes();
        let topic_id = &topic_id.to_le_bytes();
        let partition_id = &partition_id.to_le_bytes();
        let kind = &kind.to_le_bytes();
        let value = &value.to_le_bytes();
        let count = &count.to_le_bytes();

        let response = self
            .send_with_response(
                [
                    COMMAND,
                    stream_id,
                    topic_id,
                    partition_id,
                    kind,
                    value,
                    count,
                ]
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
        let offset = u64::from_le_bytes(response[position..position + 8].try_into().unwrap());
        let timestamp =
            u64::from_le_bytes(response[position + 8..position + 16].try_into().unwrap());
        let message_length =
            u64::from_le_bytes(response[position + 16..position + 24].try_into().unwrap()) as usize;
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
