use crate::client::UserClient;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::http::HttpTransport;
use crate::identifier::Identifier;
use crate::models::identity_info::IdentityInfo;
use crate::models::permissions::Permissions;
use crate::models::user_info::{UserInfo, UserInfoDetails};
use crate::models::user_status::UserStatus;
use crate::users::change_password::ChangePassword;
use crate::users::create_user::CreateUser;
use crate::users::login_user::LoginUser;
use crate::users::update_permissions::UpdatePermissions;
use crate::users::update_user::UpdateUser;
use async_trait::async_trait;

const PATH: &str = "/users";

#[async_trait]
impl UserClient for HttpClient {
    async fn get_user(&self, user_id: &Identifier) -> Result<UserInfoDetails, IggyError> {
        let response = self.get(&format!("{PATH}/{}", user_id)).await?;
        let user = response.json().await?;
        Ok(user)
    }

    async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError> {
        let response = self.get(PATH).await?;
        let users = response.json().await?;
        Ok(users)
    }

    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.post(
            PATH,
            &CreateUser {
                username: username.to_string(),
                password: password.to_string(),
                status,
                permissions,
            },
        )
        .await?;
        Ok(())
    }

    async fn delete_user(&self, user_id: &Identifier) -> Result<(), IggyError> {
        self.delete(&format!("{PATH}/{}", &user_id.as_cow_str()))
            .await?;
        Ok(())
    }

    async fn update_user(
        &self,
        user_id: &Identifier,
        username: Option<&str>,
        status: Option<UserStatus>,
    ) -> Result<(), IggyError> {
        self.put(
            &format!("{PATH}/{}", &user_id.as_cow_str()),
            &UpdateUser {
                user_id: user_id.clone(),
                username: username.map(|s| s.to_string()),
                status,
            },
        )
        .await?;
        Ok(())
    }

    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.put(
            &format!("{PATH}/{}/permissions", &user_id.as_cow_str()),
            &UpdatePermissions {
                user_id: user_id.clone(),
                permissions,
            },
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
        self.put(
            &format!("{PATH}/{}/password", &user_id.as_cow_str()),
            &ChangePassword {
                user_id: user_id.clone(),
                current_password: current_password.to_string(),
                new_password: new_password.to_string(),
            },
        )
        .await?;
        Ok(())
    }

    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, IggyError> {
        let response = self
            .post(
                &format!("{PATH}/login"),
                &LoginUser {
                    username: username.to_string(),
                    password: password.to_string(),
                },
            )
            .await?;
        let identity_info = response.json().await?;
        self.set_tokens_from_identity(&identity_info).await?;
        Ok(identity_info)
    }

    async fn logout_user(&self) -> Result<(), IggyError> {
        self.delete(&format!("{PATH}/logout")).await?;
        self.set_access_token(None).await;
        self.set_refresh_token(None).await;
        Ok(())
    }
}
