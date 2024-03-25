use crate::binary::binary_client::{BinaryClient, BinaryClientNext};
use crate::binary::{fail_if_not_authenticated, mapper, BinaryTransport, ClientState};
use crate::bytes_serializable::BytesSerializable;
use crate::client::UserClient;
use crate::command::*;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::models::identity_info::IdentityInfo;
use crate::models::permissions::Permissions;
use crate::models::user_info::{UserInfo, UserInfoDetails};
use crate::models::user_status::UserStatus;
use crate::next_client::UserClientNext;
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
        get_user(self, command).await
    }

    async fn get_users(&self, command: &GetUsers) -> Result<Vec<UserInfo>, IggyError> {
        get_users(self, command).await
    }

    async fn create_user(&self, command: &CreateUser) -> Result<(), IggyError> {
        create_user(self, command).await
    }

    async fn delete_user(&self, command: &DeleteUser) -> Result<(), IggyError> {
        delete_user(self, command).await
    }

    async fn update_user(&self, command: &UpdateUser) -> Result<(), IggyError> {
        update_user(self, command).await
    }

    async fn update_permissions(&self, command: &UpdatePermissions) -> Result<(), IggyError> {
        update_permissions(self, command).await
    }

    async fn change_password(&self, command: &ChangePassword) -> Result<(), IggyError> {
        change_password(self, command).await
    }

    async fn login_user(&self, command: &LoginUser) -> Result<IdentityInfo, IggyError> {
        login_user(self, command).await
    }

    async fn logout_user(&self, command: &LogoutUser) -> Result<(), IggyError> {
        logout_user(self, command).await
    }
}

#[async_trait::async_trait]
impl<B: BinaryClientNext> UserClientNext for B {
    async fn get_user(&self, user_id: &Identifier) -> Result<UserInfoDetails, IggyError> {
        get_user(
            self,
            &GetUser {
                user_id: user_id.clone(),
            },
        )
        .await
    }

    async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError> {
        get_users(self, &GetUsers {}).await
    }

    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: Option<UserStatus>,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        create_user(
            self,
            &CreateUser {
                username: username.to_string(),
                password: password.to_string(),
                status: status.unwrap_or(UserStatus::Active),
                permissions,
            },
        )
        .await
    }

    async fn delete_user(&self, user_id: &Identifier) -> Result<(), IggyError> {
        delete_user(
            self,
            &DeleteUser {
                user_id: user_id.clone(),
            },
        )
        .await
    }

    async fn update_user(
        &self,
        user_id: &Identifier,
        username: Option<&str>,
        status: Option<UserStatus>,
    ) -> Result<(), IggyError> {
        update_user(
            self,
            &UpdateUser {
                user_id: user_id.clone(),
                username: username.map(|s| s.to_string()),
                status,
            },
        )
        .await
    }

    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        update_permissions(
            self,
            &UpdatePermissions {
                user_id: user_id.clone(),
                permissions,
            },
        )
        .await
    }

    async fn change_password(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        change_password(
            self,
            &ChangePassword {
                user_id: user_id.clone(),
                current_password: current_password.to_string(),
                new_password: new_password.to_string(),
            },
        )
        .await
    }

    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, IggyError> {
        login_user(
            self,
            &LoginUser {
                username: username.to_string(),
                password: password.to_string(),
            },
        )
        .await
    }

    async fn logout_user(&self) -> Result<(), IggyError> {
        logout_user(self, &LogoutUser {}).await
    }
}

async fn get_user<T: BinaryTransport>(
    transport: &T,
    command: &GetUser,
) -> Result<UserInfoDetails, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(GET_USER_CODE, command.as_bytes())
        .await?;
    mapper::map_user(response)
}

async fn get_users<T: BinaryTransport>(
    transport: &T,
    command: &GetUsers,
) -> Result<Vec<UserInfo>, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(GET_USERS_CODE, command.as_bytes())
        .await?;
    mapper::map_users(response)
}

async fn create_user<T: BinaryTransport>(
    transport: &T,
    command: &CreateUser,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(CREATE_USER_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn delete_user<T: BinaryTransport>(
    transport: &T,
    command: &DeleteUser,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(DELETE_USER_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn update_user<T: BinaryTransport>(
    transport: &T,
    command: &UpdateUser,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(UPDATE_USER_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn update_permissions<T: BinaryTransport>(
    transport: &T,
    command: &UpdatePermissions,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(UPDATE_PERMISSIONS_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn change_password<T: BinaryTransport>(
    transport: &T,
    command: &ChangePassword,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(CHANGE_PASSWORD_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn login_user<T: BinaryTransport>(
    transport: &T,
    command: &LoginUser,
) -> Result<IdentityInfo, IggyError> {
    let response = transport
        .send_with_response(LOGIN_USER_CODE, command.as_bytes())
        .await?;
    transport.set_state(ClientState::Authenticated).await;
    mapper::map_identity_info(response)
}

async fn logout_user<T: BinaryTransport>(
    transport: &T,
    command: &LogoutUser,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(LOGOUT_USER_CODE, command.as_bytes())
        .await?;
    transport.set_state(ClientState::Connected).await;
    Ok(())
}
