use iggy::client::Client;
use iggy::client_error::ClientError;
use iggy::users::login_user::LoginUser;

pub async fn login_user(command: &LoginUser, client: &dyn Client) -> Result<(), ClientError> {
    client.login_user(command).await?;
    Ok(())
}
