use crate::client::SystemClient;
use crate::error::Error;
use crate::http::client::HttpClient;
use async_trait::async_trait;
use shared::system::kill::Kill;
use shared::system::ping::Ping;

const PING: &str = "/ping";
const KILL: &str = "/kill";

#[async_trait]
impl SystemClient for HttpClient {
    async fn ping(&self, _command: &Ping) -> Result<(), Error> {
        self.get(PING).await?;
        Ok(())
    }

    async fn kill(&self, _command: &Kill) -> Result<(), Error> {
        self.post(KILL, &_command).await?;
        Ok(())
    }
}
