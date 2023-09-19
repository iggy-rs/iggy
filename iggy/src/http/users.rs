use crate::client::UserClient;
use crate::error::Error;
use crate::http::client::HttpClient;
use crate::models::identity_info::IdentityInfo;
use crate::models::user_info::{UserInfo, UserInfoDetails};
use crate::users::change_password::ChangePassword;
use crate::users::create_user::CreateUser;
use crate::users::delete_user::DeleteUser;
use crate::users::get_user::GetUser;
use crate::users::get_users::GetUsers;
use crate::users::login_user::LoginUser;
use crate::users::logout_user::LogoutUser;
use crate::users::update_permissions::UpdatePermissions;
use crate::users::update_user::UpdateUser;
use async_trait::async_trait;

const PATH: &str = "/users";

#[async_trait]
impl UserClient for HttpClient {
    async fn get_user(&self, command: &GetUser) -> Result<UserInfoDetails, Error> {
        let response = self.get(&format!("{PATH}/{}", command.user_id)).await?;
        let user = response.json().await?;
        Ok(user)
    }

    async fn get_users(&self, _command: &GetUsers) -> Result<Vec<UserInfo>, Error> {
        let response = self.get(PATH).await?;
        let users = response.json().await?;
        Ok(users)
    }

    async fn create_user(&self, command: &CreateUser) -> Result<(), Error> {
        self.post(PATH, &command).await?;
        Ok(())
    }

    async fn delete_user(&self, command: &DeleteUser) -> Result<(), Error> {
        self.delete(&format!("{PATH}/{}", command.user_id)).await?;
        Ok(())
    }

    async fn update_user(&self, command: &UpdateUser) -> Result<(), Error> {
        self.put(&format!("{PATH}/{}", command.user_id), &command)
            .await?;
        Ok(())
    }

    async fn update_permissions(&self, command: &UpdatePermissions) -> Result<(), Error> {
        self.put(&format!("{PATH}/{}/permissions", command.user_id), &command)
            .await?;
        Ok(())
    }

    async fn change_password(&self, command: &ChangePassword) -> Result<(), Error> {
        self.put(&format!("{PATH}/{}/password", command.user_id), &command)
            .await?;
        Ok(())
    }

    async fn login_user(&self, command: &LoginUser) -> Result<IdentityInfo, Error> {
        let response = self.post(&format!("{PATH}/login"), &command).await?;
        let identity_info = response.json().await?;
        Ok(identity_info)
    }

    async fn logout_user(&self, command: &LogoutUser) -> Result<(), Error> {
        self.post(&format!("{PATH}/logout"), &command).await?;
        Ok(())
    }
}
