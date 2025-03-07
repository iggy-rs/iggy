use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::mapper;
use crate::http::shared::AppState;
use crate::http::COMPONENT;
use crate::streaming::session::Session;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Extension, Json, Router};
use error_set::ErrContext;
use iggy::identifier::Identifier;
use iggy::models::stream::{Stream, StreamDetails};
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::streams::purge_stream::PurgeStream;
use iggy::streams::update_stream::UpdateStream;
use iggy::validatable::Validatable;

use crate::state::command::EntryCommand;
use std::sync::Arc;
use tracing::instrument;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/streams", get(get_streams).post(create_stream))
        .route(
            "/streams/{stream_id}",
            get(get_stream).put(update_stream).delete(delete_stream),
        )
        .route("/streams/{stream_id}/purge", delete(purge_stream))
        .with_state(state)
}

async fn get_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<Json<StreamDetails>, CustomError> {
    let system = state.system.read().await;
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let Ok(stream) = system.try_find_stream(
        &Session::stateless(identity.user_id, identity.ip_address),
        &stream_id,
    ) else {
        return Err(CustomError::ResourceNotFound);
    };
    let Some(stream) = stream else {
        return Err(CustomError::ResourceNotFound);
    };

    let stream = mapper::map_stream(stream);
    Ok(Json(stream))
}

async fn get_streams(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<Stream>>, CustomError> {
    let system = state.system.read().await;
    let streams = system
        .find_streams(&Session::stateless(identity.user_id, identity.ip_address))
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to find streams, user ID: {}",
                identity.user_id
            )
        })?;
    let streams = mapper::map_streams(&streams);
    Ok(Json(streams))
}

#[instrument(skip_all, name = "trace_create_stream", fields(iggy_user_id = identity.user_id))]
async fn create_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Json(command): Json<CreateStream>,
) -> Result<Json<StreamDetails>, CustomError> {
    command.validate()?;

    let mut system = state.system.write().await;
    let stream = system
        .create_stream(
            &Session::stateless(identity.user_id, identity.ip_address),
            command.stream_id,
            &command.name,
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to create stream, stream ID: {:?}",
                command.stream_id
            )
        })?;
    let response = Json(mapper::map_stream(stream));

    let system = system.downgrade();
    let stream_id = command.stream_id;
    system
        .state
        .apply(identity.user_id, &EntryCommand::CreateStream(command))
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply create stream, stream ID: {:?}",
                stream_id
            )
        })?;
    Ok(response)
}

#[instrument(skip_all, name = "trace_update_stream", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id))]
async fn update_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
    Json(mut command): Json<UpdateStream>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.validate()?;

    let mut system = state.system.write().await;
    system
        .update_stream(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.stream_id,
            &command.name,
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to update stream, stream ID: {}",
                stream_id
            )
        })?;

    let system = system.downgrade();
    system
        .state
        .apply(identity.user_id, &EntryCommand::UpdateStream(command))
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply update stream, stream ID: {}",
                stream_id
            )
        })?;
    Ok(StatusCode::NO_CONTENT)
}

#[instrument(skip_all, name = "trace_delete_stream", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id))]
async fn delete_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let identifier_stream_id = Identifier::from_str_value(&stream_id)?;

    let mut system = state.system.write().await;
    system
        .delete_stream(
            &Session::stateless(identity.user_id, identity.ip_address),
            &identifier_stream_id,
        )
        .await
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to delete stream with ID: {stream_id}",)
        })?;

    let system = system.downgrade();
    system
        .state
        .apply(
            identity.user_id,
            &EntryCommand::DeleteStream(DeleteStream {
                stream_id: identifier_stream_id,
            }),
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply delete stream with ID: {stream_id}",
            )
        })?;
    Ok(StatusCode::NO_CONTENT)
}

#[instrument(skip_all, name = "trace_purge_stream", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id))]
async fn purge_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let identifier_stream_id = Identifier::from_str_value(&stream_id)?;
    let system = state.system.read().await;
    system
        .purge_stream(
            &Session::stateless(identity.user_id, identity.ip_address),
            &identifier_stream_id,
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to purge stream, stream ID: {}",
                stream_id
            )
        })?;
    system
        .state
        .apply(
            identity.user_id,
            &EntryCommand::PurgeStream(PurgeStream {
                stream_id: identifier_stream_id,
            }),
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply purge stream, stream ID: {}",
                stream_id
            )
        })?;
    Ok(StatusCode::NO_CONTENT)
}
