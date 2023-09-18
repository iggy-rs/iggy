use crate::http::error::CustomError;
use crate::http::jwt::Identity;
use crate::http::state::AppState;
use crate::http::{auth, mapper};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Extension, Json, Router};
use iggy::identifier::Identifier;
use iggy::models::identity_token::IdentityToken;
use iggy::models::user_info::{UserInfo, UserInfoDetails};
use iggy::users::change_password::ChangePassword;
use iggy::users::create_user::CreateUser;
use iggy::users::login_user::LoginUser;
use iggy::users::logout_user::LogoutUser;
use iggy::users::update_permissions::UpdatePermissions;
use iggy::users::update_user::UpdateUser;
use iggy::validatable::Validatable;
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
        .with_state(state)
}

async fn get_user(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<UserInfoDetails>, CustomError> {
    let user_id = Identifier::from_str_value(&user_id)?;
    let system = state.system.read().await;
    let user = system.get_user(&user_id).await?;
    if user.id != identity.user_id {
        system.permissioner.get_user(identity.user_id)?;
    }
    let user = mapper::map_user(&user);
    Ok(Json(user))
}

async fn get_users(State(state): State<Arc<AppState>>) -> Result<Json<Vec<UserInfo>>, CustomError> {
    let user_id = auth::resolve_user_id();
    let system = state.system.read().await;
    system.permissioner.get_users(user_id)?;
    let users = system.get_users().await?;
    let users = mapper::map_users(&users);
    Ok(Json(users))
}

async fn create_user(
    State(state): State<Arc<AppState>>,
    Json(command): Json<CreateUser>,
) -> Result<StatusCode, CustomError> {
    command.validate()?;
    let user_id = auth::resolve_user_id();
    let system = state.system.read().await;
    system.permissioner.create_user(user_id)?;
    system
        .create_user(
            &command.username,
            &command.password,
            command.permissions.clone(),
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn update_user(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
    Json(mut command): Json<UpdateUser>,
) -> Result<StatusCode, CustomError> {
    command.user_id = Identifier::from_str_value(&user_id)?;
    command.validate()?;
    let authenticated_user_id = auth::resolve_user_id();
    let system = state.system.read().await;
    system.permissioner.update_user(authenticated_user_id)?;
    system
        .update_user(&command.user_id, command.username, command.status)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn update_permissions(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
    Json(mut command): Json<UpdatePermissions>,
) -> Result<StatusCode, CustomError> {
    command.user_id = Identifier::from_str_value(&user_id)?;
    command.validate()?;
    let authenticated_user_id = auth::resolve_user_id();
    let mut system = state.system.write().await;
    system
        .permissioner
        .update_permissions(authenticated_user_id)?;
    system
        .update_permissions(&command.user_id, command.permissions)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn change_password(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
    Json(mut command): Json<ChangePassword>,
) -> Result<StatusCode, CustomError> {
    command.user_id = Identifier::from_str_value(&user_id)?;
    command.validate()?;
    let authenticated_user_id = auth::resolve_user_id();
    let system = state.system.read().await;
    system.permissioner.change_password(authenticated_user_id)?;
    system
        .change_password(
            &command.user_id,
            &command.current_password,
            &command.new_password,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_user(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let user_id = Identifier::from_str_value(&user_id)?;
    let authenticated_user_id = auth::resolve_user_id();
    let mut system = state.system.write().await;
    system.permissioner.delete_user(authenticated_user_id)?;
    system.delete_user(&user_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn login_user(
    State(state): State<Arc<AppState>>,
    Json(command): Json<LoginUser>,
) -> Result<Json<IdentityToken>, CustomError> {
    command.validate()?;
    let system = state.system.read().await;
    let user = system
        .login_user(&command.username, &command.password, None)
        .await?;
    Ok(Json(IdentityToken {
        token: Some(state.jwt_manager.generate(user.id)),
        user_id: user.id,
    }))
}

async fn logout_user(
    State(state): State<Arc<AppState>>,
    Json(command): Json<LogoutUser>,
) -> Result<StatusCode, CustomError> {
    command.validate()?;
    let user_id = auth::resolve_user_id();
    let system = state.system.read().await;
    system.logout_user(user_id, None).await?;
    // TODO: Clear JWT
    Ok(StatusCode::NO_CONTENT)
}
