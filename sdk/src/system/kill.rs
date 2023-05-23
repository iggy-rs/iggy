use crate::client::ConnectedClient;
use crate::error::Error;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::system::kill::Kill;

impl ConnectedClient {
    pub async fn kill(&self, command: &Kill) -> Result<(), Error> {
        self.send_with_response(
            [Command::Kill.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
        Ok(())
    }
}
