use crate::client::UserClient;
use crate::error::Error;
use crate::http::client::HttpClient;
use crate::models::identity_info::IdentityInfo;
use crate::models::pat::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use crate::models::user_info::{UserInfo, UserInfoDetails};
use crate::users::change_password::ChangePassword;
use crate::users::create_pat::CreatePersonalAccessToken;
use crate::users::create_user::CreateUser;
use crate::users::delete_pat::DeletePersonalAccessToken;
use crate::users::delete_user::DeleteUser;
use crate::users::get_pats::GetPersonalAccessTokens;
use crate::users::get_user::GetUser;
use crate::users::get_users::GetUsers;
use crate::users::login_pat::LoginWithPersonalAccessToken;
use crate::users::login_user::LoginUser;
use crate::users::logout_user::LogoutUser;
use crate::users::update_permissions::UpdatePermissions;
use crate::users::update_user::UpdateUser;
use async_trait::async_trait;

const USERS_PATH: &str = "/users";
const PAT_PATH: &str = "/pat";

#[async_trait]
impl UserClient for HttpClient {
    async fn get_user(&self, command: &GetUser) -> Result<UserInfoDetails, Error> {
        let response = self
            .get(&format!("{USERS_PATH}/{}", command.user_id))
            .await?;
        let user = response.json().await?;
        Ok(user)
    }

    async fn get_users(&self, _command: &GetUsers) -> Result<Vec<UserInfo>, Error> {
        let response = self.get(USERS_PATH).await?;
        let users = response.json().await?;
        Ok(users)
    }

    async fn create_user(&self, command: &CreateUser) -> Result<(), Error> {
        self.post(USERS_PATH, &command).await?;
        Ok(())
    }

    async fn delete_user(&self, command: &DeleteUser) -> Result<(), Error> {
        self.delete(&format!("{USERS_PATH}/{}", command.user_id))
            .await?;
        Ok(())
    }

    async fn update_user(&self, command: &UpdateUser) -> Result<(), Error> {
        self.put(&format!("{USERS_PATH}/{}", command.user_id), &command)
            .await?;
        Ok(())
    }

    async fn update_permissions(&self, command: &UpdatePermissions) -> Result<(), Error> {
        self.put(
            &format!("{USERS_PATH}/{}/permissions", command.user_id),
            &command,
        )
        .await?;
        Ok(())
    }

    async fn change_password(&self, command: &ChangePassword) -> Result<(), Error> {
        self.put(
            &format!("{USERS_PATH}/{}/password", command.user_id),
            &command,
        )
        .await?;
        Ok(())
    }

    async fn login_user(&self, command: &LoginUser) -> Result<IdentityInfo, Error> {
        let response = self.post(&format!("{USERS_PATH}/login"), &command).await?;
        let identity_info: IdentityInfo = response.json().await?;
        self.set_token(identity_info.token.clone()).await;
        Ok(identity_info)
    }

    async fn logout_user(&self, command: &LogoutUser) -> Result<(), Error> {
        self.post(&format!("{USERS_PATH}/logout"), &command).await?;
        self.set_token(None).await;
        Ok(())
    }

    async fn get_personal_access_tokens(
        &self,
        _command: &GetPersonalAccessTokens,
    ) -> Result<Vec<PersonalAccessTokenInfo>, Error> {
        let response = self.get(PAT_PATH).await?;
        let pats = response.json().await?;
        Ok(pats)
    }

    async fn create_personal_access_token(
        &self,
        command: &CreatePersonalAccessToken,
    ) -> Result<RawPersonalAccessToken, Error> {
        let response = self.post(PAT_PATH, &command).await?;
        let pat: RawPersonalAccessToken = response.json().await?;
        Ok(pat)
    }

    async fn delete_personal_access_token(
        &self,
        command: &DeletePersonalAccessToken,
    ) -> Result<(), Error> {
        self.delete(&format!("{PAT_PATH}/{}", command.name)).await?;
        Ok(())
    }

    async fn login_with_personal_access_token(
        &self,
        command: &LoginWithPersonalAccessToken,
    ) -> Result<IdentityInfo, Error> {
        let response = self.post(&format!("{PAT_PATH}/login"), &command).await?;
        let identity_info: IdentityInfo = response.json().await?;
        self.set_token(identity_info.token.clone()).await;
        Ok(identity_info)
    }
}
