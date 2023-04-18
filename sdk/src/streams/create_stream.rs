use crate::client::Client;
use crate::error::Error;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::streams::create_stream::CreateStream;

impl Client {
    pub async fn create_stream(&mut self, command: &CreateStream) -> Result<(), Error> {
        self.send_with_response(
            [Command::CreateStream.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
        Ok(())
    }
}
