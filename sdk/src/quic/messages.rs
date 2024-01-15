use crate::binary;
use crate::client::MessageClient;
use crate::error::Error;
use crate::messages::poll_messages::PollMessages;
use crate::messages::send_messages::SendMessages;
use crate::models::messages::PolledMessages;
use crate::quic::client::QuicClient;
use async_trait::async_trait;

#[async_trait]
impl MessageClient for QuicClient {
    async fn poll_messages(&self, command: &PollMessages) -> Result<PolledMessages, Error> {
        binary::messages::poll_messages(self, command).await
    }

    async fn send_messages(&self, command: &mut SendMessages) -> Result<(), Error> {
        binary::messages::send_messages(self, command).await
    }
}
