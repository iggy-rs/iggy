use crate::error::Error;
use crate::quic::client::ConnectedClient;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::streams::create_stream::CreateStream;

impl ConnectedClient {
    pub async fn create_stream(&self, command: &CreateStream) -> Result<(), Error> {
        self.send_with_response(
            [Command::CreateStream.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
        Ok(())
    }
}
