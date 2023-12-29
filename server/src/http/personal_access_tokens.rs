use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::mapper;
use crate::http::mapper::map_generated_tokens_to_identity_info;
use crate::http::shared::AppState;
use crate::streaming::session::Session;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use iggy::models::identity_info::IdentityInfo;
use iggy::models::personal_access_token::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use iggy::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy::validatable::Validatable;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/personal-access-tokens",
            get(get_personal_access_tokens).post(create_personal_access_token),
        )
        .route(
            "/personal-access-tokens/:name",
            delete(delete_personal_access_token),
        )
        .route(
            "/personal-access-tokens/login",
            post(login_with_personal_access_token),
        )
        .with_state(state)
}

async fn get_personal_access_tokens(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<PersonalAccessTokenInfo>>, CustomError> {
    let system = state.system.read();
    let personal_access_tokens = system
        .get_personal_access_tokens(&Session::stateless(identity.user_id, identity.ip_address))
        .await?;
    let personal_access_tokens = mapper::map_personal_access_tokens(&personal_access_tokens);
    Ok(Json(personal_access_tokens))
}

async fn create_personal_access_token(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Json(command): Json<CreatePersonalAccessToken>,
) -> Result<Json<RawPersonalAccessToken>, CustomError> {
    command.validate()?;
    let system = state.system.read();
    let token = system
        .create_personal_access_token(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.name,
            command.expiry,
        )
        .await?;
    Ok(Json(RawPersonalAccessToken { token }))
}

async fn delete_personal_access_token(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(name): Path<String>,
) -> Result<StatusCode, CustomError> {
    let system = state.system.read();
    system
        .delete_personal_access_token(
            &Session::stateless(identity.user_id, identity.ip_address),
            &name,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn login_with_personal_access_token(
    State(state): State<Arc<AppState>>,
    Json(command): Json<LoginWithPersonalAccessToken>,
) -> Result<Json<IdentityInfo>, CustomError> {
    command.validate()?;
    let system = state.system.read();
    let user = system
        .login_with_personal_access_token(&command.token, None)
        .await?;
    let tokens = state.jwt_manager.generate(user.id)?;
    Ok(Json(map_generated_tokens_to_identity_info(tokens)))
}
