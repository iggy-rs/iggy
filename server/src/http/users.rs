use crate::http::auth;
use crate::http::error::CustomError;
use crate::streaming::systems::system::System;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{post, put};
use axum::{Json, Router};
use iggy::identifier::Identifier;
use iggy::users::create_user::CreateUser;
use iggy::users::login_user::LoginUser;
use iggy::users::logout_user::LogoutUser;
use iggy::users::update_user::UpdateUser;
use iggy::validatable::Validatable;
use std::sync::Arc;
use tokio::sync::RwLock;

pub fn router(system: Arc<RwLock<System>>) -> Router {
    Router::new()
        .route("/", post(create_user))
        .route("/:user_id", put(update_user).delete(delete_user))
        .route("/login", post(login_user))
        .route("/logout", post(logout_user))
        .with_state(system)
}

async fn create_user(
    State(system): State<Arc<RwLock<System>>>,
    Json(command): Json<CreateUser>,
) -> Result<StatusCode, CustomError> {
    command.validate()?;
    let user_id = auth::resolve_user_id();
    let system = system.read().await;
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
    State(system): State<Arc<RwLock<System>>>,
    Path(user_id): Path<String>,
    Json(mut command): Json<UpdateUser>,
) -> Result<StatusCode, CustomError> {
    command.user_id = Identifier::from_str_value(&user_id)?;
    command.validate()?;
    let authenticated_user_id = auth::resolve_user_id();
    let system = system.read().await;
    system.permissioner.update_user(authenticated_user_id)?;
    system
        .update_user(&command.user_id, command.username, command.status)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_user(
    State(system): State<Arc<RwLock<System>>>,
    Path(user_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let user_id = Identifier::from_str_value(&user_id)?;
    let authenticated_user_id = auth::resolve_user_id();
    let mut system = system.write().await;
    system.permissioner.delete_user(authenticated_user_id)?;
    system.delete_user(&user_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn login_user(
    State(system): State<Arc<RwLock<System>>>,
    Json(command): Json<LoginUser>,
) -> Result<StatusCode, CustomError> {
    command.validate()?;
    let system = system.read().await;
    system
        .login_user(&command.username, &command.password)
        .await?;
    // TODO: Return JWT
    Ok(StatusCode::OK)
}

async fn logout_user(
    State(system): State<Arc<RwLock<System>>>,
    Json(command): Json<LogoutUser>,
) -> Result<StatusCode, CustomError> {
    command.validate()?;
    let user_id = auth::resolve_user_id();
    let system = system.read().await;
    system.logout_user(user_id).await?;
    // TODO: Clear JWT
    Ok(StatusCode::NO_CONTENT)
}
