use crate::binary::binary_client::{BinaryClient, ClientState};
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::bytes_serializable::BytesSerializable;
use crate::command::*;
use crate::error::Error;
use crate::models::identity_info::IdentityInfo;
use crate::models::personal_access_token::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use crate::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use crate::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use crate::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use crate::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;

pub async fn get_personal_access_tokens(
    client: &dyn BinaryClient,
    command: &GetPersonalAccessTokens,
) -> Result<Vec<PersonalAccessTokenInfo>, Error> {
    fail_if_not_authenticated(client).await?;
    let response = client
        .send_with_response(GET_PERSONAL_ACCESS_TOKENS_CODE, &command.as_bytes())
        .await?;
    mapper::map_personal_access_tokens(&response)
}

pub async fn create_personal_access_token(
    client: &dyn BinaryClient,
    command: &CreatePersonalAccessToken,
) -> Result<RawPersonalAccessToken, Error> {
    fail_if_not_authenticated(client).await?;
    let response = client
        .send_with_response(CREATE_PERSONAL_ACCESS_TOKEN_CODE, &command.as_bytes())
        .await?;
    mapper::map_raw_pat(&response)
}

pub async fn delete_personal_access_token(
    client: &dyn BinaryClient,
    command: &DeletePersonalAccessToken,
) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(DELETE_PERSONAL_ACCESS_TOKEN_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn login_with_personal_access_token(
    client: &dyn BinaryClient,
    command: &LoginWithPersonalAccessToken,
) -> Result<IdentityInfo, Error> {
    let response = client
        .send_with_response(LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE, &command.as_bytes())
        .await?;
    client.set_state(ClientState::Authenticated).await;
    mapper::map_identity_info(&response)
}
