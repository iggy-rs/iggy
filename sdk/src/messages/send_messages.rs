use crate::client::ConnectedClient;
use crate::error::Error;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::messages::send_messages::SendMessages;

impl ConnectedClient {
    pub async fn send_messages(&self, command: &SendMessages) -> Result<(), Error> {
        self.send_with_response(
            [Command::SendMessages.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
        Ok(())
    }
}
