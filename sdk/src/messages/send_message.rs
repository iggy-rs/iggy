use crate::client::ConnectedClient;
use crate::error::Error;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::messages::send_message::SendMessage;

impl ConnectedClient {
    pub async fn send_message(&mut self, command: &SendMessage) -> Result<(), Error> {
        self.send_with_response(
            [Command::SendMessage.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
        Ok(())
    }
}
