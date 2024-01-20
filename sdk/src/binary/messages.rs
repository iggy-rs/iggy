use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::bytes_serializable::BytesSerializable;
use crate::client::MessageClient;
use crate::command::{POLL_MESSAGES_CODE, SEND_MESSAGES_CODE};
use crate::error::Error;
use crate::messages::poll_messages::PollMessages;
use crate::messages::send_messages::SendMessages;
use crate::models::messages::PolledMessages;

#[async_trait::async_trait]
impl<B: BinaryClient> MessageClient for B {
    async fn poll_messages(&self, command: &PollMessages) -> Result<PolledMessages, Error> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(POLL_MESSAGES_CODE, &command.as_bytes())
            .await?;
        mapper::map_polled_messages(&response)
    }

    async fn send_messages(&self, command: &mut SendMessages) -> Result<(), Error> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(SEND_MESSAGES_CODE, &command.as_bytes())
            .await?;
        Ok(())
    }
}
