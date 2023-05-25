use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use sdk::stream::Stream;
use shared::streams::create_stream::CreateStream;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::Mutex;

pub async fn get_streams(
    State(system): State<Arc<Mutex<System>>>,
) -> (StatusCode, Json<Vec<Stream>>) {
    let system = system.lock().await;
    let streams = system.get_streams();
    let streams = streams
        .iter()
        .map(|stream| Stream {
            id: stream.id,
            name: stream.name.clone(),
            topics: stream.get_topics().len() as u32,
        })
        .collect();
    (StatusCode::OK, Json(streams))
}

//TODO: Error handling middleware, request payload validation etc.
pub async fn create_stream(
    State(system): State<Arc<Mutex<System>>>,
    Json(command): Json<CreateStream>,
) -> StatusCode {
    let mut system = system.lock().await;
    if system
        .create_stream(command.stream_id, &command.name)
        .await
        .is_err()
    {
        return StatusCode::BAD_REQUEST;
    }
    StatusCode::CREATED
}

pub async fn delete_stream(
    State(system): State<Arc<Mutex<System>>>,
    Path(stream_id): Path<u32>,
) -> StatusCode {
    let mut system = system.lock().await;
    if system.delete_stream(stream_id).await.is_err() {
        return StatusCode::BAD_REQUEST;
    }
    StatusCode::NO_CONTENT
}
