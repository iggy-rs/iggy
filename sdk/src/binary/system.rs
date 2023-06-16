use crate::binary::binary_client::BinaryClient;
use crate::error::Error;
use shared::bytes_serializable::BytesSerializable;
use shared::command::{KILL_CODE, PING_CODE};
use shared::system::kill::Kill;
use shared::system::ping::Ping;

pub async fn ping(client: &dyn BinaryClient, command: &Ping) -> Result<(), Error> {
    client
        .send_with_response(PING_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn kill(client: &dyn BinaryClient, command: &Kill) -> Result<(), Error> {
    client
        .send_with_response(KILL_CODE, &command.as_bytes())
        .await?;
    Ok(())
}
