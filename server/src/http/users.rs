use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::mapper;
use crate::http::mapper::map_generated_access_token_to_identity_info;
use crate::http::shared::AppState;
use crate::http::COMPONENT;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::utils::crypto;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post, put};
use axum::{Extension, Json, Router};
use error_set::ErrContext;
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
use tracing::instrument;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/users", get(get_users).post(create_user))
        .route(
            "/users/{user_id}",
            get(get_user).put(update_user).delete(delete_user),
        )
        .route("/users/{user_id}/permissions", put(update_permissions))
        .route("/users/{user_id}/password", put(change_password))
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
    let identifier_user_id = Identifier::from_str_value(&user_id)?;
    let system = state.system.read().await;
    let Ok(user) = system.find_user(
        &Session::stateless(identity.user_id, identity.ip_address),
        &identifier_user_id,
    ) else {
        return Err(CustomError::ResourceNotFound);
    };
    let Some(user) = user else {
        return Err(CustomError::ResourceNotFound);
    };

    let user = mapper::map_user(user);
    Ok(Json(user))
}

async fn get_users(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<UserInfo>>, CustomError> {
    let system = state.system.read().await;
    let users = system
        .get_users(&Session::stateless(identity.user_id, identity.ip_address))
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to get users, user ID: {}",
                identity.user_id
            )
        })?;
    let users = mapper::map_users(&users);
    Ok(Json(users))
}

#[instrument(skip_all, name = "trace_create_user", fields(iggy_user_id = identity.user_id))]
async fn create_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Json(command): Json<CreateUser>,
) -> Result<Json<UserInfoDetails>, CustomError> {
    command.validate()?;

    let mut system = state.system.write().await;
    let user = system
        .create_user(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.username,
            &command.password,
            command.status,
            command.permissions.clone(),
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to create user, username: {}",
                command.username
            )
        })?;
    let response = Json(mapper::map_user(user));

    // For the security of the system, we hash the password before storing it in metadata.
    let system = system.downgrade();
    system
        .state
        .apply(
            identity.user_id,
            &EntryCommand::CreateUser(CreateUser {
                username: command.username.clone(),
                password: crypto::hash_password(&command.password),
                status: command.status,
                permissions: command.permissions,
            }),
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply create user, username: {}",
                command.username
            )
        })?;

    Ok(response)
}

#[instrument(skip_all, name = "trace_update_user", fields(iggy_user_id = identity.user_id, iggy_updated_user_id = user_id))]
async fn update_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
    Json(mut command): Json<UpdateUser>,
) -> Result<StatusCode, CustomError> {
    command.user_id = Identifier::from_str_value(&user_id)?;
    command.validate()?;

    let mut system = state.system.write().await;
    system
        .update_user(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.user_id,
            command.username.clone(),
            command.status,
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to update user, user ID: {}",
                user_id
            )
        })?;

    let system = system.downgrade();
    system
        .state
        .apply(identity.user_id, &EntryCommand::UpdateUser(command))
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply update user, user ID: {}",
                user_id
            )
        })?;
    Ok(StatusCode::NO_CONTENT)
}

#[instrument(skip_all, name = "trace_update_permissions", fields(iggy_user_id = identity.user_id, iggy_updated_user_id = user_id))]
async fn update_permissions(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
    Json(mut command): Json<UpdatePermissions>,
) -> Result<StatusCode, CustomError> {
    command.user_id = Identifier::from_str_value(&user_id)?;
    command.validate()?;

    let mut system = state.system.write().await;
    system
        .update_permissions(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.user_id,
            command.permissions.clone(),
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to update permissions, user ID: {}",
                user_id
            )
        })?;

    let system = system.downgrade();
    system
        .state
        .apply(identity.user_id, &EntryCommand::UpdatePermissions(command))
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply update permissions, user ID: {}",
                user_id
            )
        })?;
    Ok(StatusCode::NO_CONTENT)
}

#[instrument(skip_all, name = "trace_change_password", fields(iggy_user_id = identity.user_id, iggy_updated_user_id = user_id))]
async fn change_password(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
    Json(mut command): Json<ChangePassword>,
) -> Result<StatusCode, CustomError> {
    command.user_id = Identifier::from_str_value(&user_id)?;
    command.validate()?;

    let mut system = state.system.write().await;
    system
        .change_password(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.user_id,
            &command.current_password,
            &command.new_password,
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to change password, user ID: {}",
                user_id
            )
        })?;

    // For the security of the system, we hash the password before storing it in metadata.
    let system = system.downgrade();
    system
        .state
        .apply(
            identity.user_id,
            &EntryCommand::ChangePassword(ChangePassword {
                user_id: command.user_id,
                current_password: "".into(),
                new_password: crypto::hash_password(&command.new_password),
            }),
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply change password, user ID: {}",
                user_id
            )
        })?;
    Ok(StatusCode::NO_CONTENT)
}

#[instrument(skip_all, name = "trace_delete_user", fields(iggy_user_id = identity.user_id, iggy_deleted_user_id = user_id))]
async fn delete_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let identifier_user_id = Identifier::from_str_value(&user_id)?;

    let mut system = state.system.write().await;
    system
        .delete_user(
            &Session::stateless(identity.user_id, identity.ip_address),
            &identifier_user_id,
        )
        .await
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to delete user with ID: {user_id}")
        })?;

    let system = system.downgrade();
    system
        .state
        .apply(
            identity.user_id,
            &EntryCommand::DeleteUser(DeleteUser {
                user_id: identifier_user_id,
            }),
        )
        .await
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to apply delete user with ID: {user_id}")
        })?;
    Ok(StatusCode::NO_CONTENT)
}

#[instrument(skip_all, name = "trace_login_user")]
async fn login_user(
    State(state): State<Arc<AppState>>,
    Json(command): Json<LoginUser>,
) -> Result<Json<IdentityInfo>, CustomError> {
    command.validate()?;
    let system = state.system.read().await;
    let user = system
        .login_user(&command.username, &command.password, None)
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to login, username: {}",
                command.username
            )
        })?;
    let tokens = state.jwt_manager.generate(user.id)?;
    Ok(Json(map_generated_access_token_to_identity_info(tokens)))
}

#[instrument(skip_all, name = "trace_logout_user", fields(iggy_user_id = identity.user_id))]
async fn logout_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<StatusCode, CustomError> {
    let system = state.system.read().await;
    system
        .logout_user(&Session::stateless(identity.user_id, identity.ip_address))
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to logout, user ID: {}",
                identity.user_id
            )
        })?;
    state
        .jwt_manager
        .revoke_token(&identity.token_id, identity.token_expiry)
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to revoke token, user ID: {}",
                identity.user_id
            )
        })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn refresh_token(
    State(state): State<Arc<AppState>>,
    Json(command): Json<RefreshToken>,
) -> Result<Json<IdentityInfo>, CustomError> {
    let token = state
        .jwt_manager
        .refresh_token(&command.token)
        .await
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to refresh token")
        })?;
    Ok(Json(map_generated_access_token_to_identity_info(token)))
}

#[derive(Debug, Deserialize)]
struct RefreshToken {
    token: String,
}
