use crate::client::Client;
use crate::error::Error;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;

impl Client {
    pub async fn ping(&mut self) -> Result<(), Error> {
        self.send_with_response(&Command::Ping.as_bytes()).await?;
        Ok(())
    }
}
