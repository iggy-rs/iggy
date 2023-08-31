use crate::binary::binary_client::BinaryClient;
use crate::binary::mapper;
use crate::bytes_serializable::BytesSerializable;
use crate::command::{POLL_MESSAGES_CODE, SEND_MESSAGES_CODE};
use crate::error::Error;
use crate::messages::poll_messages::PollMessages;
use crate::messages::send_messages::SendMessages;
use crate::models::messages::PolledMessages;

pub async fn poll_messages(
    client: &dyn BinaryClient,
    command: &PollMessages,
) -> Result<PolledMessages, Error> {
    let response = client
        .send_with_response(POLL_MESSAGES_CODE, &command.as_bytes())
        .await?;
    mapper::map_polled_messages(&response)
}

pub async fn send_messages(client: &dyn BinaryClient, command: &SendMessages) -> Result<(), Error> {
    client
        .send_with_response(SEND_MESSAGES_CODE, &command.as_bytes())
        .await?;
    Ok(())
}
