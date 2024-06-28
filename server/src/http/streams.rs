use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::mapper;
use crate::http::shared::AppState;
use crate::streaming::session::Session;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Extension, Json, Router};
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::{
    CREATE_STREAM_CODE, DELETE_STREAM_CODE, PURGE_STREAM_CODE, UPDATE_STREAM_CODE,
};
use iggy::identifier::Identifier;
use iggy::models::stream::{Stream, StreamDetails};
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::streams::purge_stream::PurgeStream;
use iggy::streams::update_stream::UpdateStream;
use iggy::validatable::Validatable;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/streams", get(get_streams).post(create_stream))
        .route(
            "/streams/:stream_id",
            get(get_stream).put(update_stream).delete(delete_stream),
        )
        .route("/streams/:stream_id/purge", delete(purge_stream))
        .with_state(state)
}

async fn get_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<Json<StreamDetails>, CustomError> {
    let system = state.system.read();
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let stream = system.find_stream(
        &Session::stateless(identity.user_id, identity.ip_address),
        &stream_id,
    )?;
    let stream = mapper::map_stream(stream).await;
    Ok(Json(stream))
}

async fn get_streams(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<Stream>>, CustomError> {
    let system = state.system.read();
    let streams =
        system.find_streams(&Session::stateless(identity.user_id, identity.ip_address))?;
    let streams = mapper::map_streams(&streams).await;
    Ok(Json(streams))
}

async fn create_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Json(command): Json<CreateStream>,
) -> Result<StatusCode, CustomError> {
    command.validate()?;
    let mut system = state.system.write();
    system
        .create_stream(
            &Session::stateless(identity.user_id, identity.ip_address),
            command.stream_id,
            &command.name,
        )
        .await?;
    system
        .state
        .apply(
            CREATE_STREAM_CODE,
            identity.user_id,
            &command.to_bytes(),
            None,
        )
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
    let mut system = state.system.write();
    system
        .update_stream(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.stream_id,
            &command.name,
        )
        .await?;
    system
        .state
        .apply(
            UPDATE_STREAM_CODE,
            identity.user_id,
            &command.to_bytes(),
            None,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let mut system = state.system.write();
    system
        .delete_stream(
            &Session::stateless(identity.user_id, identity.ip_address),
            &stream_id,
        )
        .await?;
    system
        .state
        .apply(
            DELETE_STREAM_CODE,
            identity.user_id,
            &DeleteStream { stream_id }.to_bytes(),
            None,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn purge_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let system = state.system.read();
    system
        .purge_stream(
            &Session::stateless(identity.user_id, identity.ip_address),
            &stream_id,
        )
        .await?;
    system
        .state
        .apply(
            PURGE_STREAM_CODE,
            identity.user_id,
            &PurgeStream { stream_id }.to_bytes(),
            None,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
