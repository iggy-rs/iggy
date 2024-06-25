use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper, ClientState};
use crate::bytes_serializable::BytesSerializable;
use crate::client::UserClient;
use crate::command::*;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::models::identity_info::IdentityInfo;
use crate::models::permissions::Permissions;
use crate::models::user_info::{UserInfo, UserInfoDetails};
use crate::models::user_status::UserStatus;
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
    async fn get_user(&self, user_id: &Identifier) -> Result<UserInfoDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(
                GET_USER_CODE,
                GetUser {
                    user_id: user_id.clone(),
                }
                .as_bytes(),
            )
            .await?;
        mapper::map_user(response)
    }

    async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(GET_USERS_CODE, GetUsers {}.as_bytes())
            .await?;
        mapper::map_users(response)
    }

    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(
            CREATE_USER_CODE,
            CreateUser {
                username: username.to_string(),
                password: password.to_string(),
                status,
                permissions,
            }
            .as_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn delete_user(&self, user_id: &Identifier) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(
            DELETE_USER_CODE,
            DeleteUser {
                user_id: user_id.clone(),
            }
            .as_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn update_user(
        &self,
        user_id: &Identifier,
        username: Option<&str>,
        status: Option<UserStatus>,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(
            UPDATE_USER_CODE,
            UpdateUser {
                user_id: user_id.clone(),
                username: username.map(|s| s.to_string()),
                status,
            }
            .as_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(
            UPDATE_PERMISSIONS_CODE,
            UpdatePermissions {
                user_id: user_id.clone(),
                permissions,
            }
            .as_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn change_password(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(
            CHANGE_PASSWORD_CODE,
            ChangePassword {
                user_id: user_id.clone(),
                current_password: current_password.to_string(),
                new_password: new_password.to_string(),
            }
            .as_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, IggyError> {
        let response = self
            .send_with_response(
                LOGIN_USER_CODE,
                LoginUser {
                    username: username.to_string(),
                    password: password.to_string(),
                    version: Some("0.5.0".to_string()),
                    context: Some("".to_string()),
                }
                .as_bytes(),
            )
            .await?;
        self.set_state(ClientState::Authenticated).await;
        mapper::map_identity_info(response)
    }

    async fn logout_user(&self) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(LOGOUT_USER_CODE, LogoutUser {}.as_bytes())
            .await?;
        self.set_state(ClientState::Connected).await;
        Ok(())
    }
}
