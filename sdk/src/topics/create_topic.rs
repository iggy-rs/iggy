use crate::client::ConnectedClient;
use crate::error::Error;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::topics::create_topic::CreateTopic;

impl ConnectedClient {
    pub async fn create_topic(&mut self, command: &CreateTopic) -> Result<(), Error> {
        self.send_with_response(
            [Command::CreateTopic.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
        Ok(())
    }
}
