use crate::client::PersonalAccessTokenClient;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::http::HttpTransport;
use crate::models::identity_info::IdentityInfo;
use crate::models::personal_access_token::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use crate::next_client::PersonalAccessTokenClientNext;
use crate::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use crate::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use crate::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use crate::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use crate::utils::personal_access_token_expiry::PersonalAccessTokenExpiry;
use async_trait::async_trait;

const PATH: &str = "/personal-access-tokens";

#[async_trait]
impl PersonalAccessTokenClient for HttpClient {
    async fn get_personal_access_tokens(
        &self,
        command: &GetPersonalAccessTokens,
    ) -> Result<Vec<PersonalAccessTokenInfo>, IggyError> {
        get_personal_access_tokens(self, command).await
    }

    async fn create_personal_access_token(
        &self,
        command: &CreatePersonalAccessToken,
    ) -> Result<RawPersonalAccessToken, IggyError> {
        create_personal_access_token(self, command).await
    }

    async fn delete_personal_access_token(
        &self,
        command: &DeletePersonalAccessToken,
    ) -> Result<(), IggyError> {
        delete_personal_access_token(self, command).await
    }

    async fn login_with_personal_access_token(
        &self,
        command: &LoginWithPersonalAccessToken,
    ) -> Result<IdentityInfo, IggyError> {
        login_with_personal_access_token(self, command).await
    }
}

#[async_trait]
impl PersonalAccessTokenClientNext for HttpClient {
    async fn get_personal_access_tokens(&self) -> Result<Vec<PersonalAccessTokenInfo>, IggyError> {
        get_personal_access_tokens(self, &GetPersonalAccessTokens {}).await
    }

    async fn create_personal_access_token(
        &self,
        name: &str,
        expiry: PersonalAccessTokenExpiry,
    ) -> Result<RawPersonalAccessToken, IggyError> {
        create_personal_access_token(
            self,
            &CreatePersonalAccessToken {
                name: name.to_string(),
                expiry: expiry.into(),
            },
        )
        .await
    }

    async fn delete_personal_access_token(&self, name: &str) -> Result<(), IggyError> {
        delete_personal_access_token(
            self,
            &DeletePersonalAccessToken {
                name: name.to_string(),
            },
        )
        .await
    }

    async fn login_with_personal_access_token(
        &self,
        token: &str,
    ) -> Result<IdentityInfo, IggyError> {
        login_with_personal_access_token(
            self,
            &LoginWithPersonalAccessToken {
                token: token.to_string(),
            },
        )
        .await
    }
}

async fn get_personal_access_tokens<T: HttpTransport>(
    transport: &T,
    _command: &GetPersonalAccessTokens,
) -> Result<Vec<PersonalAccessTokenInfo>, IggyError> {
    let response = transport.get(PATH).await?;
    let personal_access_tokens = response.json().await?;
    Ok(personal_access_tokens)
}

async fn create_personal_access_token<T: HttpTransport>(
    transport: &T,
    command: &CreatePersonalAccessToken,
) -> Result<RawPersonalAccessToken, IggyError> {
    let response = transport.post(PATH, &command).await?;
    let personal_access_token: RawPersonalAccessToken = response.json().await?;
    Ok(personal_access_token)
}

async fn delete_personal_access_token<T: HttpTransport>(
    transport: &T,
    command: &DeletePersonalAccessToken,
) -> Result<(), IggyError> {
    transport
        .delete(&format!("{PATH}/{}", command.name))
        .await?;
    Ok(())
}

async fn login_with_personal_access_token<T: HttpTransport>(
    transport: &T,
    command: &LoginWithPersonalAccessToken,
) -> Result<IdentityInfo, IggyError> {
    let response = transport.post(&format!("{PATH}/login"), &command).await?;
    let identity_info: IdentityInfo = response.json().await?;
    transport.set_tokens_from_identity(&identity_info).await?;
    Ok(identity_info)
}
