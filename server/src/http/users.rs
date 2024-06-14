use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::mapper;
use crate::http::mapper::map_generated_access_token_to_identity_info;
use crate::http::shared::AppState;
use crate::streaming::session::Session;
use crate::streaming::utils::crypto;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post, put};
use axum::{Extension, Json, Router};
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::{
    CHANGE_PASSWORD_CODE, CREATE_USER_CODE, DELETE_USER_CODE, UPDATE_PERMISSIONS_CODE,
    UPDATE_USER_CODE,
};
use iggy::identifier::Identifier;
use iggy::models::identity_info::IdentityInfo;
use iggy::models::user_info::{UserInfo, UserInfoDetails};
use iggy::users::change_password::ChangePassword;
use iggy::users::create_user::CreateUser;
use iggy::users::delete_user::DeleteUser;
use iggy::users::login_user::LoginUser;
use iggy::users::update_permissions::UpdatePermissions;
use iggy::users::update_user::UpdateUser;
use iggy::validatable::Validatable;
use serde::Deserialize;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/users", get(get_users).post(create_user))
        .route(
            "/users/:user_id",
            get(get_user).put(update_user).delete(delete_user),
        )
        .route("/users/:user_id/permissions", put(update_permissions))
        .route("/users/:user_id/password", put(change_password))
        .route("/users/login", post(login_user))
        .route("/users/logout", delete(logout_user))
        .route("/users/refresh-token", post(refresh_token))
        .with_state(state)
}

async fn get_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
) -> Result<Json<UserInfoDetails>, CustomError> {
    let user_id = Identifier::from_str_value(&user_id)?;
    let system = state.system.read();
    let user = system.find_user(
        &Session::stateless(identity.user_id, identity.ip_address),
        &user_id,
    )?;
    let user = mapper::map_user(user);
    Ok(Json(user))
}

async fn get_users(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<UserInfo>>, CustomError> {
    let system = state.system.read();
    let users = system
        .get_users(&Session::stateless(identity.user_id, identity.ip_address))
        .await?;
    let users = mapper::map_users(&users);
    Ok(Json(users))
}

async fn create_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Json(command): Json<CreateUser>,
) -> Result<StatusCode, CustomError> {
    command.validate()?;
    let mut system = state.system.write();
    system
        .create_user(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.username,
            &command.password,
            command.status,
            command.permissions.clone(),
        )
        .await?;

    // For the security of the system, we hash the password before storing it in metadata.
    system
        .metadata
        .apply(
            CREATE_USER_CODE,
            identity.user_id,
            &CreateUser {
                username: command.username.to_owned(),
                password: crypto::hash_password(&command.password),
                status: command.status,
                permissions: command.permissions.clone(),
            }
            .as_bytes(),
            None,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn update_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
    Json(mut command): Json<UpdateUser>,
) -> Result<StatusCode, CustomError> {
    command.user_id = Identifier::from_str_value(&user_id)?;
    command.validate()?;
    let bytes = command.as_bytes();
    let mut system = state.system.write();
    system
        .update_user(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.user_id,
            command.username,
            command.status,
        )
        .await?;
    system
        .metadata
        .apply(UPDATE_USER_CODE, identity.user_id, &bytes, None)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn update_permissions(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
    Json(mut command): Json<UpdatePermissions>,
) -> Result<StatusCode, CustomError> {
    command.user_id = Identifier::from_str_value(&user_id)?;
    command.validate()?;
    let bytes = command.as_bytes();
    let mut system = state.system.write();
    system
        .update_permissions(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.user_id,
            command.permissions,
        )
        .await?;
    system
        .metadata
        .apply(UPDATE_PERMISSIONS_CODE, identity.user_id, &bytes, None)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn change_password(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
    Json(mut command): Json<ChangePassword>,
) -> Result<StatusCode, CustomError> {
    command.user_id = Identifier::from_str_value(&user_id)?;
    command.validate()?;
    let mut system = state.system.write();
    system
        .change_password(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.user_id,
            &command.current_password,
            &command.new_password,
        )
        .await?;
    // For the security of the system, we hash the password before storing it in metadata.
    system
        .metadata
        .apply(
            CHANGE_PASSWORD_CODE,
            identity.user_id,
            &ChangePassword {
                user_id: command.user_id.to_owned(),
                current_password: "".into(),
                new_password: crypto::hash_password(&command.new_password),
            }
            .as_bytes(),
            None,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let user_id = Identifier::from_str_value(&user_id)?;
    let mut system = state.system.write();
    system
        .delete_user(
            &Session::stateless(identity.user_id, identity.ip_address),
            &user_id,
        )
        .await?;
    system
        .metadata
        .apply(
            DELETE_USER_CODE,
            identity.user_id,
            &DeleteUser { user_id }.as_bytes(),
            None,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn login_user(
    State(state): State<Arc<AppState>>,
    Json(command): Json<LoginUser>,
) -> Result<Json<IdentityInfo>, CustomError> {
    command.validate()?;
    let system = state.system.read();
    let user = system
        .login_user(&command.username, &command.password, None)
        .await?;
    let tokens = state.jwt_manager.generate(user.id)?;
    Ok(Json(map_generated_access_token_to_identity_info(tokens)))
}

async fn logout_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<StatusCode, CustomError> {
    let system = state.system.read();
    system
        .logout_user(&Session::stateless(identity.user_id, identity.ip_address))
        .await?;
    state
        .jwt_manager
        .revoke_token(&identity.token_id, identity.token_expiry)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn refresh_token(
    State(state): State<Arc<AppState>>,
    Json(command): Json<RefreshToken>,
) -> Result<Json<IdentityInfo>, CustomError> {
    let token = state.jwt_manager.refresh_token(&command.token).await?;
    Ok(Json(map_generated_access_token_to_identity_info(token)))
}

#[derive(Debug, Deserialize)]
struct RefreshToken {
    token: String,
}
