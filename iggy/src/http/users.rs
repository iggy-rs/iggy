use crate::client::UserClient;
use crate::error::Error;
use crate::http::client::HttpClient;
use crate::users::login_user::LoginUser;
use async_trait::async_trait;

const PATH: &str = "/users";

#[async_trait]
impl UserClient for HttpClient {
    async fn login_user(&self, command: &LoginUser) -> Result<(), Error> {
        self.post(&format!("{}/login", PATH), &command).await?;
        Ok(())
    }
}
