use iggy::client::Client;
use iggy::client_error::ClientError;
use iggy::users::login_user::LoginUser;
use iggy::users::logout_user::LogoutUser;

pub async fn login_user(command: &LoginUser, client: &dyn Client) -> Result<(), ClientError> {
    client.login_user(command).await?;
    Ok(())
}

pub async fn logout_user(command: &LogoutUser, client: &dyn Client) -> Result<(), ClientError> {
    client.logout_user(command).await?;
    Ok(())
}
