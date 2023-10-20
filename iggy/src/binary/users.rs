use crate::binary::binary_client::{BinaryClient, ClientState};
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::bytes_serializable::BytesSerializable;
use crate::command::*;
use crate::error::Error;
use crate::models::identity_info::IdentityInfo;
use crate::models::user_info::{UserInfo, UserInfoDetails};
use crate::users::change_password::ChangePassword;
use crate::users::create_user::CreateUser;
use crate::users::delete_user::DeleteUser;
use crate::users::get_user::GetUser;
use crate::users::get_users::GetUsers;
use crate::users::login_user::LoginUser;
use crate::users::logout_user::LogoutUser;
use crate::users::update_permissions::UpdatePermissions;
use crate::users::update_user::UpdateUser;

pub async fn get_user(
    client: &dyn BinaryClient,
    command: &GetUser,
) -> Result<UserInfoDetails, Error> {
    fail_if_not_authenticated(client).await?;
    let response = client
        .send_with_response(GET_USER_CODE, &command.as_bytes())
        .await?;
    mapper::map_user(&response)
}

pub async fn get_users(
    client: &dyn BinaryClient,
    command: &GetUsers,
) -> Result<Vec<UserInfo>, Error> {
    fail_if_not_authenticated(client).await?;
    let response = client
        .send_with_response(GET_USERS_CODE, &command.as_bytes())
        .await?;
    mapper::map_users(&response)
}

pub async fn create_user(client: &dyn BinaryClient, command: &CreateUser) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(CREATE_USER_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn delete_user(client: &dyn BinaryClient, command: &DeleteUser) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(DELETE_USER_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn update_user(client: &dyn BinaryClient, command: &UpdateUser) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(UPDATE_USER_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn update_permissions(
    client: &dyn BinaryClient,
    command: &UpdatePermissions,
) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(UPDATE_PERMISSIONS_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn change_password(
    client: &dyn BinaryClient,
    command: &ChangePassword,
) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(CHANGE_PASSWORD_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn login_user(
    client: &dyn BinaryClient,
    command: &LoginUser,
) -> Result<IdentityInfo, Error> {
    let response = client
        .send_with_response(LOGIN_USER_CODE, &command.as_bytes())
        .await?;
    client.set_state(ClientState::Authenticated).await;
    mapper::map_identity_info(&response)
}

pub async fn logout_user(client: &dyn BinaryClient, command: &LogoutUser) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(LOGOUT_USER_CODE, &command.as_bytes())
        .await?;
    client.set_state(ClientState::Connected).await;
    Ok(())
}
