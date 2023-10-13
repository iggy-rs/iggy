use iggy::client::Client;
use iggy::client_error::ClientError;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use iggy::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use iggy::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use iggy::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use tracing::info;

pub async fn get_personal_access_tokens(
    command: &GetPersonalAccessTokens,
    client: &dyn Client,
) -> Result<(), ClientError> {
    let personal_access_tokens = client.get_personal_access_tokens(command).await?;
    info!("Personal access tokens: {:#?}", personal_access_tokens);
    Ok(())
}

pub async fn create_personal_access_token(
    command: &CreatePersonalAccessToken,
    client: &dyn Client,
) -> Result<(), ClientError> {
    let personal_access_token = client.create_personal_access_token(command).await?;
    info!("Personal access token: {}", personal_access_token.token);
    Ok(())
}

pub async fn delete_personal_access_token(
    command: &DeletePersonalAccessToken,
    client: &dyn Client,
) -> Result<(), ClientError> {
    client.delete_personal_access_token(command).await?;
    Ok(())
}

pub async fn login_with_personal_access_token(
    command: &LoginWithPersonalAccessToken,
    client: &dyn Client,
) -> Result<(), ClientError> {
    client.login_with_personal_access_token(command).await?;
    Ok(())
}
