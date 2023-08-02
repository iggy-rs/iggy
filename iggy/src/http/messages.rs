use crate::client::MessageClient;
use crate::error::Error;
use crate::http::client::HttpClient;
use crate::identifier::{IdKind, Identifier};
use crate::messages::poll_messages::PollMessages;
use crate::messages::send_messages::SendMessages;
use crate::models::message::Message;
use crate::models::offset::Offset;
use crate::offsets::get_offset::GetOffset;
use crate::offsets::store_offset::StoreOffset;
use async_trait::async_trait;

#[async_trait]
impl MessageClient for HttpClient {
    async fn poll_messages(&self, command: &PollMessages) -> Result<Vec<Message>, Error> {
        let stream_id = identifier_to_string(&command.stream_id);
        let topic_id = identifier_to_string(&command.topic_id);
        let response = self
            .get_with_query(&get_messages_path(&stream_id, &topic_id), &command)
            .await?;
        let messages = response.json().await?;
        Ok(messages)
    }

    async fn send_messages(&self, command: &SendMessages) -> Result<(), Error> {
        let stream_id = identifier_to_string(&command.stream_id);
        let topic_id = identifier_to_string(&command.topic_id);
        self.post(&get_messages_path(&stream_id, &topic_id), &command)
            .await?;
        Ok(())
    }

    async fn store_offset(&self, command: &StoreOffset) -> Result<(), Error> {
        self.put(
            &get_offsets_path(command.stream_id, command.topic_id),
            &command,
        )
        .await?;
        Ok(())
    }

    async fn get_offset(&self, command: &GetOffset) -> Result<Offset, Error> {
        let response = self
            .get_with_query(
                &get_offsets_path(command.stream_id, command.topic_id),
                &command,
            )
            .await?;
        let offset = response.json().await?;
        Ok(offset)
    }
}

fn identifier_to_string(identifier: &Identifier) -> String {
    match identifier.kind {
        IdKind::Numeric => {
            u32::from_le_bytes(identifier.value.clone().try_into().unwrap()).to_string()
        }
        IdKind::String => String::from_utf8_lossy(&identifier.value).to_string(),
    }
}

fn get_offsets_path(stream_id: u32, topic_id: u32) -> String {
    format!("streams/{}/topics/{}/messages/offsets", stream_id, topic_id)
}

fn get_messages_path(stream_id: &str, topic_id: &str) -> String {
    format!("streams/{}/topics/{}/messages", stream_id, topic_id)
}
