use crate::client::UserClient;
use crate::error::Error;
use crate::http::client::HttpClient;
use crate::users::create_user::CreateUser;
use crate::users::login_user::LoginUser;
use crate::users::logout_user::LogoutUser;
use async_trait::async_trait;

const PATH: &str = "/users";

#[async_trait]
impl UserClient for HttpClient {
    async fn create_user(&self, command: &CreateUser) -> Result<(), Error> {
        self.post(PATH, &command).await?;
        Ok(())
    }

    async fn login_user(&self, command: &LoginUser) -> Result<(), Error> {
        self.post(&format!("{PATH}/login"), &command).await?;
        Ok(())
    }

    async fn logout_user(&self, command: &LogoutUser) -> Result<(), Error> {
        self.post(&format!("{PATH}/logout"), &command).await?;
        Ok(())
    }
}
