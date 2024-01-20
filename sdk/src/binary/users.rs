use crate::binary::binary_client::{BinaryClient, ClientState};
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::bytes_serializable::BytesSerializable;
use crate::client::UserClient;
use crate::command::*;
use crate::error::IggyError;
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

#[async_trait::async_trait]
impl<B: BinaryClient> UserClient for B {
    async fn get_user(&self, command: &GetUser) -> Result<UserInfoDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(GET_USER_CODE, &command.as_bytes())
            .await?;
        mapper::map_user(&response)
    }

    async fn get_users(&self, command: &GetUsers) -> Result<Vec<UserInfo>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(GET_USERS_CODE, &command.as_bytes())
            .await?;
        mapper::map_users(&response)
    }

    async fn create_user(&self, command: &CreateUser) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(CREATE_USER_CODE, &command.as_bytes())
            .await?;
        Ok(())
    }

    async fn delete_user(&self, command: &DeleteUser) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(DELETE_USER_CODE, &command.as_bytes())
            .await?;
        Ok(())
    }

    async fn update_user(&self, command: &UpdateUser) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(UPDATE_USER_CODE, &command.as_bytes())
            .await?;
        Ok(())
    }

    async fn update_permissions(&self, command: &UpdatePermissions) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(UPDATE_PERMISSIONS_CODE, &command.as_bytes())
            .await?;
        Ok(())
    }

    async fn change_password(&self, command: &ChangePassword) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(CHANGE_PASSWORD_CODE, &command.as_bytes())
            .await?;
        Ok(())
    }

    async fn login_user(&self, command: &LoginUser) -> Result<IdentityInfo, IggyError> {
        let response = self
            .send_with_response(LOGIN_USER_CODE, &command.as_bytes())
            .await?;
        self.set_state(ClientState::Authenticated).await;
        mapper::map_identity_info(&response)
    }

    async fn logout_user(&self, command: &LogoutUser) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(LOGOUT_USER_CODE, &command.as_bytes())
            .await?;
        self.set_state(ClientState::Connected).await;
        Ok(())
    }
}
