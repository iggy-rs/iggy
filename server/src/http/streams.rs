use crate::http::error::CustomError;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Json, Router};
use sdk::stream::Stream;
use shared::streams::create_stream::CreateStream;
use shared::validatable::Validatable;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;

pub fn router(system: Arc<RwLock<System>>) -> Router {
    Router::new()
        .route("/", get(get_streams).post(create_stream))
        .route("/:stream_id", delete(delete_stream))
        .with_state(system)
}

async fn get_streams(
    State(system): State<Arc<RwLock<System>>>,
) -> Result<Json<Vec<Stream>>, CustomError> {
    let streams = system
        .write()
        .await
        .get_streams()
        .iter()
        .map(|stream| Stream {
            id: stream.id,
            name: stream.name.clone(),
            topics: stream.get_topics().len() as u32,
        })
        .collect();
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

async fn delete_stream(
    State(system): State<Arc<RwLock<System>>>,
    Path(stream_id): Path<u32>,
) -> Result<StatusCode, CustomError> {
    system.write().await.delete_stream(stream_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
