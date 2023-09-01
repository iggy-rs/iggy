use crate::http::error::CustomError;
use crate::http::mapper;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use iggy::identifier::Identifier;
use iggy::models::stream::{Stream, StreamDetails};
use iggy::streams::create_stream::CreateStream;
use iggy::streams::update_stream::UpdateStream;
use iggy::validatable::Validatable;
use std::sync::Arc;
use streaming::systems::system::System;
use tokio::sync::RwLock;

pub fn router(system: Arc<RwLock<System>>) -> Router {
    Router::new()
        .route("/", get(get_streams).post(create_stream))
        .route(
            "/:stream_id",
            get(get_stream).put(update_stream).delete(delete_stream),
        )
        .with_state(system)
}

async fn get_stream(
    State(system): State<Arc<RwLock<System>>>,
    Path(stream_id): Path<String>,
) -> Result<Json<StreamDetails>, CustomError> {
    let system = system.read().await;
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let stream = system.get_stream(&stream_id)?;
    let stream = mapper::map_stream(stream).await;
    Ok(Json(stream))
}

async fn get_streams(
    State(system): State<Arc<RwLock<System>>>,
) -> Result<Json<Vec<Stream>>, CustomError> {
    let system = system.read().await;
    let streams = mapper::map_streams(&system.get_streams()).await;
    Ok(Json(streams))
}

async fn create_stream(
    State(system): State<Arc<RwLock<System>>>,
    Json(command): Json<CreateStream>,
) -> Result<StatusCode, CustomError> {
    command.validate()?;
    let mut system = system.write().await;
    system
        .create_stream(command.stream_id, &command.name)
        .await?;
    Ok(StatusCode::CREATED)
}

async fn update_stream(
    State(system): State<Arc<RwLock<System>>>,
    Path(stream_id): Path<String>,
    Json(mut command): Json<UpdateStream>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.validate()?;
    system
        .write()
        .await
        .update_stream(&command.stream_id, &command.name)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_stream(
    State(system): State<Arc<RwLock<System>>>,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    system.write().await.delete_stream(&stream_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
