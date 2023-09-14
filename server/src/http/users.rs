use crate::http::auth;
use crate::http::error::CustomError;
use crate::streaming::systems::system::System;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use iggy::users::login_user::LoginUser;
use iggy::users::logout_user::LogoutUser;
use iggy::validatable::Validatable;
use std::sync::Arc;
use tokio::sync::RwLock;

pub fn router(system: Arc<RwLock<System>>) -> Router {
    Router::new()
        .route("/login", post(login_user))
        .route("/logout", post(logout_user))
        .with_state(system)
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
    Ok(StatusCode::OK)
}
