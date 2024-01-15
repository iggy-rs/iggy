use crate::client::PersonalAccessTokenClient;
use crate::error::Error;
use crate::http::client::HttpClient;
use crate::models::identity_info::IdentityInfo;
use crate::models::personal_access_token::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use crate::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use crate::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use crate::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use crate::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use async_trait::async_trait;

const PATH: &str = "/personal-access-tokens";

#[async_trait]
impl PersonalAccessTokenClient for HttpClient {
    async fn get_personal_access_tokens(
        &self,
        _command: &GetPersonalAccessTokens,
    ) -> Result<Vec<PersonalAccessTokenInfo>, Error> {
        let response = self.get(PATH).await?;
        let personal_access_tokens = response.json().await?;
        Ok(personal_access_tokens)
    }

    async fn create_personal_access_token(
        &self,
        command: &CreatePersonalAccessToken,
    ) -> Result<RawPersonalAccessToken, Error> {
        let response = self.post(PATH, &command).await?;
        let personal_access_token: RawPersonalAccessToken = response.json().await?;
        Ok(personal_access_token)
    }

    async fn delete_personal_access_token(
        &self,
        command: &DeletePersonalAccessToken,
    ) -> Result<(), Error> {
        self.delete(&format!("{PATH}/{}", command.name)).await?;
        Ok(())
    }

    async fn login_with_personal_access_token(
        &self,
        command: &LoginWithPersonalAccessToken,
    ) -> Result<IdentityInfo, Error> {
        let response = self.post(&format!("{PATH}/login"), &command).await?;
        let identity_info: IdentityInfo = response.json().await?;
        self.set_tokens_from_identity(&identity_info).await?;
        Ok(identity_info)
    }
}
