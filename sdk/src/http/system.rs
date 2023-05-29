use crate::error::Error;
use crate::http::client::Client;
use shared::system::kill::Kill;

const PING: &str = "/ping";
const KILL: &str = "/kill";

impl Client {
    pub async fn ping(&self) -> Result<String, Error> {
        let response = self.get(PING).await?;
        let pong = response.text().await?;
        Ok(pong)
    }

    pub async fn kill(&self) -> Result<(), Error> {
        self.post(KILL, &Kill {}).await?;
        Ok(())
    }
}
