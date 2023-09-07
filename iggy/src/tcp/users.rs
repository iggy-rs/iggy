use crate::binary;
use crate::client::UserClient;
use crate::error::Error;
use crate::tcp::client::TcpClient;
use crate::users::login_user::LoginUser;
use async_trait::async_trait;

#[async_trait]
impl UserClient for TcpClient {
    async fn login_user(&self, command: &LoginUser) -> Result<(), Error> {
        binary::users::login_user(self, command).await
    }
}
