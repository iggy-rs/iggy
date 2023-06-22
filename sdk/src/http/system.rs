use crate::client::SystemClient;
use crate::error::Error;
use crate::http::client::HttpClient;
use crate::models::client_info::ClientInfo;
use crate::system::get_clients::GetClients;
use crate::system::kill::Kill;
use crate::system::ping::Ping;
use async_trait::async_trait;

const PING: &str = "/ping";
const KILL: &str = "/kill";
const CLIENTS: &str = "/clients";

#[async_trait]
impl SystemClient for HttpClient {
    async fn get_clients(&self, _command: &GetClients) -> Result<Vec<ClientInfo>, Error> {
        let response = self.get(CLIENTS).await?;
        let clients = response.json().await?;
        Ok(clients)
    }

    async fn ping(&self, _command: &Ping) -> Result<(), Error> {
        self.get(PING).await?;
        Ok(())
    }

    async fn kill(&self, _command: &Kill) -> Result<(), Error> {
        self.post(KILL, &_command).await?;
        Ok(())
    }
}
