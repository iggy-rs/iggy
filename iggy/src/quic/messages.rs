use crate::binary;
use crate::client::MessageClient;
use crate::error::Error;
use crate::messages::poll_messages::PollMessages;
use crate::messages::send_messages::SendMessages;
use crate::models::message::Message;
use crate::models::offset::Offset;
use crate::offsets::get_offset::GetOffset;
use crate::offsets::store_offset::StoreOffset;
use crate::quic::client::QuicClient;
use async_trait::async_trait;

#[async_trait]
impl MessageClient for QuicClient {
    async fn poll_messages(&self, command: &PollMessages) -> Result<Vec<Message>, Error> {
        binary::messages::poll_messages(self, command).await
    }

    async fn send_messages(&self, command: &SendMessages) -> Result<(), Error> {
        binary::messages::send_messages(self, command).await
    }

    async fn store_offset(&self, command: &StoreOffset) -> Result<(), Error> {
        binary::messages::store_offset(self, command).await
    }

    async fn get_offset(&self, command: &GetOffset) -> Result<Offset, Error> {
        binary::messages::get_offset(self, command).await
    }
}
