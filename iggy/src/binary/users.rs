use crate::binary::binary_client::BinaryClient;
use crate::binary::mapper;
use crate::bytes_serializable::BytesSerializable;
use crate::command::{
    CHANGE_PASSWORD_CODE, CREATE_USER_CODE, DELETE_USER_CODE, GET_USERS_CODE, GET_USER_CODE,
    LOGIN_USER_CODE, LOGOUT_USER_CODE, UPDATE_PERMISSIONS_CODE, UPDATE_USER_CODE,
};
use crate::error::Error;
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
    let response = client
        .send_with_response(GET_USER_CODE, &command.as_bytes())
        .await?;
    mapper::map_user(&response)
}

pub async fn get_users(
    client: &dyn BinaryClient,
    command: &GetUsers,
) -> Result<Vec<UserInfo>, Error> {
    let response = client
        .send_with_response(GET_USERS_CODE, &command.as_bytes())
        .await?;
    mapper::map_users(&response)
}

pub async fn create_user(client: &dyn BinaryClient, command: &CreateUser) -> Result<(), Error> {
    client
        .send_with_response(CREATE_USER_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn delete_user(client: &dyn BinaryClient, command: &DeleteUser) -> Result<(), Error> {
    client
        .send_with_response(DELETE_USER_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn update_user(client: &dyn BinaryClient, command: &UpdateUser) -> Result<(), Error> {
    client
        .send_with_response(UPDATE_USER_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn update_permissions(
    client: &dyn BinaryClient,
    command: &UpdatePermissions,
) -> Result<(), Error> {
    client
        .send_with_response(UPDATE_PERMISSIONS_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn change_password(
    client: &dyn BinaryClient,
    command: &ChangePassword,
) -> Result<(), Error> {
    client
        .send_with_response(CHANGE_PASSWORD_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

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
