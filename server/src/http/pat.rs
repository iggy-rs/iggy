use crate::http::error::CustomError;
use crate::http::jwt::Identity;
use crate::http::state::AppState;
use crate::streaming::session::Session;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, post};
use axum::{Extension, Json, Router};
use iggy::models::pat::RawPersonalAccessToken;
use iggy::users::create_pat::CreatePersonalAccessToken;
use iggy::validatable::Validatable;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", post(create_pat))
        .route("/:name", delete(delete_pat))
        .with_state(state)
}

async fn create_pat(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Json(command): Json<CreatePersonalAccessToken>,
) -> Result<Json<RawPersonalAccessToken>, CustomError> {
    command.validate()?;
    let system = state.system.read().await;
    let token = system
        .create_pat(
            &Session::stateless(identity.user_id),
            &command.name,
            command.expiry,
        )
        .await?;
    Ok(Json(RawPersonalAccessToken { token }))
}

async fn delete_pat(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(name): Path<String>,
) -> Result<StatusCode, CustomError> {
    let system = state.system.read().await;
    system
        .delete_pat(&Session::stateless(identity.user_id), &name)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
