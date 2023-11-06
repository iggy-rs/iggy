use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::mapper;
use crate::http::state::AppState;
use crate::streaming::session::Session;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Extension, Json, Router};
use iggy::identifier::Identifier;
use iggy::models::identity_info::{IdentityInfo, TokenInfo};
use iggy::models::user_info::{UserInfo, UserInfoDetails};
use iggy::users::change_password::ChangePassword;
use iggy::users::create_user::CreateUser;
use iggy::users::login_user::LoginUser;
use iggy::users::logout_user::LogoutUser;
use iggy::users::update_permissions::UpdatePermissions;
use iggy::users::update_user::UpdateUser;
use iggy::validatable::Validatable;
use serde::Deserialize;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(get_users).post(create_user))
        .route(
            "/:user_id",
            get(get_user).put(update_user).delete(delete_user),
        )
        .route("/:user_id/permissions", put(update_permissions))
        .route("/:user_id/password", put(change_password))
        .route("/login", post(login_user))
        .route("/logout", post(logout_user))
        .route("/refresh-token", post(refresh_token))
        .with_state(state)
}

async fn get_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
) -> Result<Json<UserInfoDetails>, CustomError> {
    let user_id = Identifier::from_str_value(&user_id)?;
    let system = state.system.read().await;
    let user = system
        .find_user(
            &Session::stateless(identity.user_id, identity.ip_address),
            &user_id,
        )
        .await?;
    let user = mapper::map_user(&user);
    Ok(Json(user))
}

async fn get_users(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<UserInfo>>, CustomError> {
    let system = state.system.read().await;
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
    let mut system = state.system.write().await;
    system
        .create_user(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.username,
            &command.password,
            command.status,
            command.permissions.clone(),
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
    let system = state.system.read().await;
    system
        .update_user(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.user_id,
            command.username,
            command.status,
        )
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
    let mut system = state.system.write().await;
    system
        .update_permissions(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.user_id,
            command.permissions,
        )
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
    let system = state.system.read().await;
    system
        .change_password(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.user_id,
            &command.current_password,
            &command.new_password,
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
    let mut system = state.system.write().await;
    system
        .delete_user(
            &Session::stateless(identity.user_id, identity.ip_address),
            &user_id,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn login_user(
    State(state): State<Arc<AppState>>,
    Json(command): Json<LoginUser>,
) -> Result<Json<IdentityInfo>, CustomError> {
    command.validate()?;
    let system = state.system.read().await;
    let user = system
        .login_user(&command.username, &command.password, None)
        .await?;
    let token = state.jwt_manager.generate(user.id)?;
    Ok(Json(IdentityInfo {
        user_id: user.id,
        token: Some({
            TokenInfo {
                access_token: token.access_token,
                refresh_token: token.refresh_token,
                expiry: token.expiry,
            }
        }),
    }))
}

async fn logout_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Json(command): Json<LogoutUser>,
) -> Result<StatusCode, CustomError> {
    command.validate()?;
    let system = state.system.read().await;
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
    let token = state.jwt_manager.refresh_token(&command.refresh_token)?;
    Ok(Json(IdentityInfo {
        user_id: token.user_id,
        token: Some({
            TokenInfo {
                access_token: token.access_token,
                refresh_token: token.refresh_token,
                expiry: token.expiry,
            }
        }),
    }))
}

#[derive(Debug, Deserialize)]
struct RefreshToken {
    refresh_token: String,
}
