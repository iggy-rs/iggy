use crate::binary;
use crate::client::UserClient;
use crate::error::Error;
use crate::tcp::client::TcpClient;
use crate::users::create_user::CreateUser;
use crate::users::delete_user::DeleteUser;
use crate::users::login_user::LoginUser;
use crate::users::logout_user::LogoutUser;
use crate::users::update_permissions::UpdatePermissions;
use crate::users::update_user::UpdateUser;
use async_trait::async_trait;

#[async_trait]
impl UserClient for TcpClient {
    async fn create_user(&self, command: &CreateUser) -> Result<(), Error> {
        binary::users::create_user(self, command).await
    }

    async fn delete_user(&self, command: &DeleteUser) -> Result<(), Error> {
        binary::users::delete_user(self, command).await
    }

    async fn update_user(&self, command: &UpdateUser) -> Result<(), Error> {
        binary::users::update_user(self, command).await
    }

    async fn update_permissions(&self, command: &UpdatePermissions) -> Result<(), Error> {
        binary::users::update_permissions(self, command).await
    }

    async fn login_user(&self, command: &LoginUser) -> Result<(), Error> {
        binary::users::login_user(self, command).await
    }

    async fn logout_user(&self, command: &LogoutUser) -> Result<(), Error> {
        binary::users::logout_user(self, command).await
    }
}
