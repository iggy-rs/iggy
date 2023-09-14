use crate::binary::binary_client::BinaryClient;
use crate::bytes_serializable::BytesSerializable;
use crate::command::{LOGIN_USER_CODE, LOGOUT_USER_CODE};
use crate::error::Error;
use crate::users::login_user::LoginUser;
use crate::users::logout_user::LogoutUser;

pub async fn login_user(client: &dyn BinaryClient, command: &LoginUser) -> Result<(), Error> {
    client
        .send_with_response(LOGIN_USER_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn logout_user(client: &dyn BinaryClient, command: &LogoutUser) -> Result<(), Error> {
    client
        .send_with_response(LOGOUT_USER_CODE, &command.as_bytes())
        .await?;
    Ok(())
}
