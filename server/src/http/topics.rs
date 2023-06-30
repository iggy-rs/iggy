use crate::http::error::CustomError;
use crate::http::mapper;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use sdk::models::topic::{Topic, TopicDetails};
use sdk::topics::create_topic::CreateTopic;
use sdk::validatable::Validatable;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;

pub fn router(system: Arc<RwLock<System>>) -> Router {
    Router::new()
        .route("/", get(get_topics).post(create_topic))
        .route("/:topic_id", get(get_topic).delete(delete_topic))
        .with_state(system)
}

async fn get_topic(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(u32, u32)>,
) -> Result<Json<TopicDetails>, CustomError> {
    let system = system.read().await;
    let topic = system.get_stream(stream_id)?.get_topic(topic_id)?;
    let topic = mapper::map_topic(topic).await;
    Ok(Json(topic))
}

async fn get_topics(
    State(system): State<Arc<RwLock<System>>>,
    Path(stream_id): Path<u32>,
) -> Result<Json<Vec<Topic>>, CustomError> {
    let system = system.read().await;
    let topics = system.get_stream(stream_id)?.get_topics();
    let topics = mapper::map_topics(&topics);
    Ok(Json(topics))
}

async fn create_topic(
    State(system): State<Arc<RwLock<System>>>,
    Path(stream_id): Path<u32>,
    Json(mut command): Json<CreateTopic>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = stream_id;
    command.validate()?;
    let mut system = system.write().await;
    system
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
