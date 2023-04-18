use crate::client::Client;
use crate::error::Error;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::topics::delete_topic::DeleteTopic;

impl Client {
    pub async fn delete_topic(&mut self, command: &DeleteTopic) -> Result<(), Error> {
        self.send_with_response(
            [Command::DeleteTopic.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
        Ok(())
    }
}
