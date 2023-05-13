use crate::client::ConnectedClient;
use crate::error::Error;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::system::ping::Ping;

impl ConnectedClient {
    pub async fn ping(&self, command: &Ping) -> Result<(), Error> {
        self.send_with_response(
            [Command::Ping.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
        Ok(())
    }
}
