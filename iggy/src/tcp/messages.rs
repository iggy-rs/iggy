use crate::binary;
use crate::client::MessageClient;
use crate::error::Error;
use crate::messages::poll_messages::PollMessages;
use crate::messages::send_messages::SendMessages;
use crate::models::message::Message;
use crate::tcp::client::TcpClient;
use async_trait::async_trait;

#[async_trait]
impl MessageClient for TcpClient {
    async fn poll_messages(&self, command: &PollMessages) -> Result<Vec<Message>, Error> {
        binary::messages::poll_messages(self, command).await
    }

    async fn send_messages(&self, command: &SendMessages) -> Result<(), Error> {
        binary::messages::send_messages(self, command).await
    }
}
