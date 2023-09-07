use crate::http::error::CustomError;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use iggy::users::login_user::LoginUser;
use iggy::validatable::Validatable;
use std::sync::Arc;
use streaming::systems::system::System;
use tokio::sync::RwLock;

pub fn router(system: Arc<RwLock<System>>) -> Router {
    Router::new()
        .route("/login", post(login_user))
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
