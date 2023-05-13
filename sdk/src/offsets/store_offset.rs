use crate::client::ConnectedClient;
use crate::error::Error;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::offsets::store_offset::StoreOffset;

impl ConnectedClient {
    pub async fn store_offset(&self, command: &StoreOffset) -> Result<(), Error> {
        self.send_with_response(
            [Command::StoreOffset.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
        Ok(())
    }
}
