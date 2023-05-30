use crate::http::error::CustomError;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Json, Router};
use sdk::topic::Topic;
use shared::topics::create_topic::CreateTopic;
use shared::validatable::Validatable;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;

pub fn router(system: Arc<RwLock<System>>) -> Router {
    Router::new()
        .route("/", get(get_topics).post(create_topic))
        .route("/:topic_id", delete(delete_topic))
        .with_state(system)
}

async fn get_topics(
    State(system): State<Arc<RwLock<System>>>,
    Path(stream_id): Path<u32>,
) -> Result<Json<Vec<Topic>>, CustomError> {
    let topics = system
        .write()
        .await
        .get_stream(stream_id)?
        .get_topics()
        .iter()
        .map(|topic| Topic {
            id: topic.id,
            name: topic.name.clone(),
            partitions: topic.get_partitions().len() as u32,
        })
        .collect();
    Ok(Json(topics))
}

async fn create_topic(
    State(system): State<Arc<RwLock<System>>>,
    Path(stream_id): Path<u32>,
    Json(mut command): Json<CreateTopic>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = stream_id;
    command.validate()?;
    system
        .write()
        .await
        .get_stream_mut(stream_id)?
        .create_topic(command.topic_id, &command.name, command.partitions_count)
        .await?;
    Ok(StatusCode::CREATED)
}

async fn delete_topic(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(u32, u32)>,
) -> Result<StatusCode, CustomError> {
    system
        .write()
        .await
        .get_stream_mut(stream_id)?
        .delete_topic(topic_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
