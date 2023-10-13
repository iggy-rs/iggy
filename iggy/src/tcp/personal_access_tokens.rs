use crate::binary;
use crate::client::PersonalAccessTokenClient;
use crate::error::Error;
use crate::models::identity_info::IdentityInfo;
use crate::models::personal_access_token::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use crate::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use crate::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use crate::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use crate::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use crate::tcp::client::TcpClient;
use async_trait::async_trait;

#[async_trait]
impl PersonalAccessTokenClient for TcpClient {
    async fn get_personal_access_tokens(
        &self,
        command: &GetPersonalAccessTokens,
    ) -> Result<Vec<PersonalAccessTokenInfo>, Error> {
        binary::personal_access_tokens::get_personal_access_tokens(self, command).await
    }

    async fn create_personal_access_token(
        &self,
        command: &CreatePersonalAccessToken,
    ) -> Result<RawPersonalAccessToken, Error> {
        binary::personal_access_tokens::create_personal_access_token(self, command).await
    }

    async fn delete_personal_access_token(
        &self,
        command: &DeletePersonalAccessToken,
    ) -> Result<(), Error> {
        binary::personal_access_tokens::delete_personal_access_token(self, command).await
    }

    async fn login_with_personal_access_token(
        &self,
        command: &LoginWithPersonalAccessToken,
    ) -> Result<IdentityInfo, Error> {
        binary::personal_access_tokens::login_with_personal_access_token(self, command).await
    }
}
