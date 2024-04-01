use crate::client::UserClient;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::http::HttpTransport;
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
use async_trait::async_trait;

const PATH: &str = "/users";

#[async_trait]
impl UserClient for HttpClient {
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

#[async_trait]
impl UserClientNext for HttpClient {
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
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        create_user(
            self,
            &CreateUser {
                username: username.to_string(),
                password: password.to_string(),
                status,
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

async fn get_user<T: HttpTransport>(
    transport: &T,
    command: &GetUser,
) -> Result<UserInfoDetails, IggyError> {
    let response = transport
        .get(&format!("{PATH}/{}", command.user_id))
        .await?;
    let user = response.json().await?;
    Ok(user)
}

async fn get_users<T: HttpTransport>(
    transport: &T,
    _command: &GetUsers,
) -> Result<Vec<UserInfo>, IggyError> {
    let response = transport.get(PATH).await?;
    let users = response.json().await?;
    Ok(users)
}

async fn create_user<T: HttpTransport>(
    transport: &T,
    command: &CreateUser,
) -> Result<(), IggyError> {
    transport.post(PATH, &command).await?;
    Ok(())
}

async fn delete_user<T: HttpTransport>(
    transport: &T,
    command: &DeleteUser,
) -> Result<(), IggyError> {
    transport
        .delete(&format!("{PATH}/{}", command.user_id))
        .await?;
    Ok(())
}

async fn update_user<T: HttpTransport>(
    transport: &T,
    command: &UpdateUser,
) -> Result<(), IggyError> {
    transport
        .put(&format!("{PATH}/{}", command.user_id), &command)
        .await?;
    Ok(())
}

async fn update_permissions<T: HttpTransport>(
    transport: &T,
    command: &UpdatePermissions,
) -> Result<(), IggyError> {
    transport
        .put(&format!("{PATH}/{}/permissions", command.user_id), &command)
        .await?;
    Ok(())
}

async fn change_password<T: HttpTransport>(
    transport: &T,
    command: &ChangePassword,
) -> Result<(), IggyError> {
    transport
        .put(&format!("{PATH}/{}/password", command.user_id), &command)
        .await?;
    Ok(())
}

async fn login_user<T: HttpTransport>(
    transport: &T,
    command: &LoginUser,
) -> Result<IdentityInfo, IggyError> {
    let response = transport.post(&format!("{PATH}/login"), &command).await?;
    let identity_info: IdentityInfo = response.json().await?;
    transport.set_tokens_from_identity(&identity_info).await?;
    Ok(identity_info)
}

async fn logout_user<T: HttpTransport>(
    transport: &T,
    command: &LogoutUser,
) -> Result<(), IggyError> {
    transport.post(&format!("{PATH}/logout"), &command).await?;
    transport.set_access_token(None).await;
    transport.set_refresh_token(None).await;
    Ok(())
}
