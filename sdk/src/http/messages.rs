use crate::error::Error;
use crate::http::client::Client;
use crate::message::Message;
use shared::messages::poll_messages::PollMessages;
use shared::messages::send_messages::SendMessages;

impl Client {
    pub async fn poll_messages(&self, command: &PollMessages) -> Result<Vec<Message>, Error> {
        let response = self
            .get_with_query(
                &Self::get_messages_path(command.stream_id, command.topic_id),
                &command,
            )
            .await?;
        let messages = response.json().await?;
        Ok(messages)
    }

    pub async fn send_messages(&self, command: &SendMessages) -> Result<(), Error> {
        self.post(
            &Self::get_messages_path(command.stream_id, command.topic_id),
            command,
        )
        .await?;
        Ok(())
    }

    fn get_messages_path(stream_id: u32, topic_id: u32) -> String {
        format!("streams/{}/topics/{}/messages", stream_id, topic_id)
    }
}
