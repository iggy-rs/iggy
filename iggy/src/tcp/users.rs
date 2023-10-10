use crate::binary;
use crate::client::UserClient;
use crate::error::Error;
use crate::models::identity_info::IdentityInfo;
use crate::models::pat::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use crate::models::user_info::{UserInfo, UserInfoDetails};
use crate::tcp::client::TcpClient;
use crate::users::change_password::ChangePassword;
use crate::users::create_pat::CreatePersonalAccessToken;
use crate::users::create_user::CreateUser;
use crate::users::delete_pat::DeletePersonalAccessToken;
use crate::users::delete_user::DeleteUser;
use crate::users::get_pats::GetPersonalAccessTokens;
use crate::users::get_user::GetUser;
use crate::users::get_users::GetUsers;
use crate::users::login_user::LoginUser;
use crate::users::logout_user::LogoutUser;
use crate::users::update_permissions::UpdatePermissions;
use crate::users::update_user::UpdateUser;
use async_trait::async_trait;

#[async_trait]
impl UserClient for TcpClient {
    async fn get_user(&self, command: &GetUser) -> Result<UserInfoDetails, Error> {
        binary::users::get_user(self, command).await
    }

    async fn get_users(&self, command: &GetUsers) -> Result<Vec<UserInfo>, Error> {
        binary::users::get_users(self, command).await
    }

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

    async fn change_password(&self, command: &ChangePassword) -> Result<(), Error> {
        binary::users::change_password(self, command).await
    }

    async fn login_user(&self, command: &LoginUser) -> Result<IdentityInfo, Error> {
        binary::users::login_user(self, command).await
    }

    async fn logout_user(&self, command: &LogoutUser) -> Result<(), Error> {
        binary::users::logout_user(self, command).await
    }

    async fn get_personal_access_tokens(
        &self,
        command: &GetPersonalAccessTokens,
    ) -> Result<Vec<PersonalAccessTokenInfo>, Error> {
        binary::users::get_pats(self, command).await
    }

    async fn create_personal_access_token(
        &self,
        command: &CreatePersonalAccessToken,
    ) -> Result<RawPersonalAccessToken, Error> {
        binary::users::create_pat(self, command).await
    }

    async fn delete_personal_access_token(
        &self,
        command: &DeletePersonalAccessToken,
    ) -> Result<(), Error> {
        binary::users::delete_pat(self, command).await
    }
}
