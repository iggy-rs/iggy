use iggy::client::Client;
use iggy::client_error::ClientError;
use iggy::users::change_password::ChangePassword;
use iggy::users::create_user::CreateUser;
use iggy::users::delete_user::DeleteUser;
use iggy::users::get_user::GetUser;
use iggy::users::get_users::GetUsers;
use iggy::users::login_user::LoginUser;
use iggy::users::logout_user::LogoutUser;
use iggy::users::update_permissions::UpdatePermissions;
use iggy::users::update_user::UpdateUser;
use tracing::info;

pub async fn get_user(command: &GetUser, client: &dyn Client) -> Result<(), ClientError> {
    let user = client.get_user(command).await?;
    info!("User: {:#?}", user);
    Ok(())
}

pub async fn get_users(command: &GetUsers, client: &dyn Client) -> Result<(), ClientError> {
    let users = client.get_users(command).await?;
    if users.is_empty() {
        info!("No users found");
        return Ok(());
    }

    info!("Users: {:#?}", users);
    Ok(())
}

pub async fn create_user(command: &CreateUser, client: &dyn Client) -> Result<(), ClientError> {
    client.create_user(command).await?;
    Ok(())
}

pub async fn delete_user(command: &DeleteUser, client: &dyn Client) -> Result<(), ClientError> {
    client.delete_user(command).await?;
    Ok(())
}

pub async fn update_user(command: &UpdateUser, client: &dyn Client) -> Result<(), ClientError> {
    client.update_user(command).await?;
    Ok(())
}

pub async fn update_permissions(
    command: &UpdatePermissions,
    client: &dyn Client,
) -> Result<(), ClientError> {
    client.update_permissions(command).await?;
    Ok(())
}

pub async fn change_password(
    command: &ChangePassword,
    client: &dyn Client,
) -> Result<(), ClientError> {
    client.change_password(command).await?;
    Ok(())
}

pub async fn login_user(command: &LoginUser, client: &dyn Client) -> Result<(), ClientError> {
    client.login_user(command).await?;
    Ok(())
}

pub async fn logout_user(command: &LogoutUser, client: &dyn Client) -> Result<(), ClientError> {
    client.logout_user(command).await?;
    Ok(())
}
