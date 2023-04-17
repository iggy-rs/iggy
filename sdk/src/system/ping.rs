use crate::client::Client;
use crate::error::Error;

const COMMAND: &[u8] = &[1];

impl Client {
    pub async fn ping(&mut self) -> Result<(), Error> {
        self.send(COMMAND).await
    }
}
