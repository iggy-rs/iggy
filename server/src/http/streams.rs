use crate::http::error::CustomError;
use crate::http::jwt::Identity;
use crate::http::mapper;
use crate::http::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use iggy::identifier::Identifier;
use iggy::models::stream::{Stream, StreamDetails};
use iggy::streams::create_stream::CreateStream;
use iggy::streams::update_stream::UpdateStream;
use iggy::validatable::Validatable;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(get_streams).post(create_stream))
        .route(
            "/:stream_id",
            get(get_stream).put(update_stream).delete(delete_stream),
        )
        .with_state(state)
}

async fn get_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<Json<StreamDetails>, CustomError> {
    let system = state.system.read().await;
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let stream = system.get_stream(&stream_id)?;
    system
        .permissioner
        .get_stream(identity.user_id, stream.stream_id)?;
    let stream = mapper::map_stream(stream).await;
    Ok(Json(stream))
}

async fn get_streams(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<Stream>>, CustomError> {
    let system = state.system.read().await;
    system.permissioner.get_streams(identity.user_id)?;
    let streams = mapper::map_streams(&system.get_streams()).await;
    Ok(Json(streams))
}

async fn create_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Json(command): Json<CreateStream>,
) -> Result<StatusCode, CustomError> {
    command.validate()?;
    let mut system = state.system.write().await;
    system.permissioner.create_stream(identity.user_id)?;
    system
        .create_stream(command.stream_id, &command.name)
        .await?;
    Ok(StatusCode::CREATED)
}

async fn update_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
    Json(mut command): Json<UpdateStream>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.validate()?;
    let mut system = state.system.write().await;
    let stream = system.get_stream(&command.stream_id)?;
    system
        .permissioner
        .update_stream(identity.user_id, stream.stream_id)?;
    system
        .update_stream(&command.stream_id, &command.name)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let mut system = state.system.write().await;
    let stream = system.get_stream(&stream_id)?;
    system
        .permissioner
        .delete_stream(identity.user_id, stream.stream_id)?;
    system.delete_stream(&stream_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
