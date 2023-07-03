use crate::http::error::CustomError;
use crate::http::mapper;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use sdk::groups::create_group::CreateGroup;
use sdk::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use sdk::validatable::Validatable;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;

pub fn router(system: Arc<RwLock<System>>) -> Router {
    Router::new()
        .route("/", get(get_groups).post(create_group))
        .route("/:group_id", get(get_group).delete(delete_group))
        .with_state(system)
}

async fn get_group(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id, group_id)): Path<(u32, u32, u32)>,
) -> Result<Json<ConsumerGroupDetails>, CustomError> {
    let system = system.read().await;
    let consumer_group = system
        .get_stream(stream_id)?
        .get_topic(topic_id)?
        .get_consumer_group(group_id)?;
    let consumer_group = consumer_group.read().await;
    let consumer_group = mapper::map_consumer_group(&consumer_group).await;
    Ok(Json(consumer_group))
}

async fn get_groups(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(u32, u32)>,
) -> Result<Json<Vec<ConsumerGroup>>, CustomError> {
    let system = system.read().await;
    let topic = system.get_stream(stream_id)?.get_topic(topic_id)?;
    let consumer_groups = mapper::map_consumer_groups(&topic.get_consumer_groups()).await;
    Ok(Json(consumer_groups))
}

async fn create_group(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(u32, u32)>,
    Json(mut command): Json<CreateGroup>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = stream_id;
    command.topic_id = topic_id;
    command.validate()?;
    let mut system = system.write().await;
    system.create_consumer_group(stream_id, topic_id, command.group_id)?;
    Ok(StatusCode::CREATED)
}

async fn delete_group(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id, group_id)): Path<(u32, u32, u32)>,
) -> Result<StatusCode, CustomError> {
    let mut system = system.write().await;
    system
        .delete_consumer_group(stream_id, topic_id, group_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
