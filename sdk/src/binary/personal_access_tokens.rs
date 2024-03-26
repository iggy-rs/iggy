use crate::binary::binary_client::{BinaryClient, BinaryClientNext};
use crate::binary::{fail_if_not_authenticated, mapper, BinaryTransport, ClientState};
use crate::bytes_serializable::BytesSerializable;
use crate::client::PersonalAccessTokenClient;
use crate::command::*;
use crate::error::IggyError;
use crate::models::identity_info::IdentityInfo;
use crate::models::personal_access_token::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use crate::next_client::PersonalAccessTokenClientNext;
use crate::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use crate::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use crate::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use crate::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use crate::utils::personal_access_token_expiry::PersonalAccessTokenExpiry;

#[async_trait::async_trait]
impl<B: BinaryClient> PersonalAccessTokenClient for B {
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

#[async_trait::async_trait]
impl<B: BinaryClientNext> PersonalAccessTokenClientNext for B {
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

async fn get_personal_access_tokens<T: BinaryTransport>(
    transport: &T,
    command: &GetPersonalAccessTokens,
) -> Result<Vec<PersonalAccessTokenInfo>, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(GET_PERSONAL_ACCESS_TOKENS_CODE, command.as_bytes())
        .await?;
    mapper::map_personal_access_tokens(response)
}

async fn create_personal_access_token<T: BinaryTransport>(
    transport: &T,
    command: &CreatePersonalAccessToken,
) -> Result<RawPersonalAccessToken, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(CREATE_PERSONAL_ACCESS_TOKEN_CODE, command.as_bytes())
        .await?;
    mapper::map_raw_pat(response)
}

async fn delete_personal_access_token<T: BinaryTransport>(
    transport: &T,
    command: &DeletePersonalAccessToken,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(DELETE_PERSONAL_ACCESS_TOKEN_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn login_with_personal_access_token<T: BinaryTransport>(
    transport: &T,
    command: &LoginWithPersonalAccessToken,
) -> Result<IdentityInfo, IggyError> {
    let response = transport
        .send_with_response(LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE, command.as_bytes())
        .await?;
    transport.set_state(ClientState::Authenticated).await;
    mapper::map_identity_info(response)
}
