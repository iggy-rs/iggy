use crate::binary::binary_client::BinaryClient;
use crate::binary::mapper;
use crate::bytes_serializable::BytesSerializable;
use crate::command::{GET_OFFSET_CODE, POLL_MESSAGES_CODE, SEND_MESSAGES_CODE, STORE_OFFSET_CODE};
use crate::error::Error;
use crate::messages::poll_messages::PollMessages;
use crate::messages::send_messages::SendMessages;
use crate::models::message::Message;
use crate::models::offset::Offset;
use crate::offsets::get_offset::GetOffset;
use crate::offsets::store_offset::StoreOffset;

pub async fn poll_messages(
    client: &dyn BinaryClient,
    command: &PollMessages,
) -> Result<Vec<Message>, Error> {
    let response = client
        .send_with_response(POLL_MESSAGES_CODE, &command.as_bytes())
        .await?;
    mapper::map_messages(&response)
}

pub async fn send_messages(client: &dyn BinaryClient, command: &SendMessages) -> Result<(), Error> {
    client
        .send_with_response(SEND_MESSAGES_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn store_offset(client: &dyn BinaryClient, command: &StoreOffset) -> Result<(), Error> {
    client
        .send_with_response(STORE_OFFSET_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn get_offset(client: &dyn BinaryClient, command: &GetOffset) -> Result<Offset, Error> {
    let response = client
        .send_with_response(GET_OFFSET_CODE, &command.as_bytes())
        .await?;
    mapper::map_offset(&response)
}
