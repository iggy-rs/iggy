use crate::error::Error;
use crate::quic::client::ConnectedClient;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::streams::delete_stream::DeleteStream;

impl ConnectedClient {
    pub async fn delete_stream(&self, command: &DeleteStream) -> Result<(), Error> {
        self.send_with_response(
            [Command::DeleteStream.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
        Ok(())
    }
}
