use crate::http::error::CustomError;
use crate::http::jwt::Identity;
use crate::http::state::AppState;
use crate::streaming::session::Session;
use axum::extract::State;
use axum::routing::post;
use axum::{Extension, Json, Router};
use iggy::models::pat::RawPersonalAccessToken;
use iggy::users::create_pat::CreatePersonalAccessToken;
use iggy::validatable::Validatable;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new().route("/", post(create_pat)).with_state(state)
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
