use crate::http::error::CustomError;
use crate::http::mapper;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::identifier::Identifier;
use iggy::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use iggy::validatable::Validatable;
use std::sync::Arc;
use streaming::systems::system::System;
use tokio::sync::RwLock;

pub fn router(system: Arc<RwLock<System>>) -> Router {
    Router::new()
        .route("/", get(get_consumer_groups).post(create_consumer_group))
        .route(
            "/:consumer_group_id",
            get(get_consumer_group).delete(delete_consumer_group),
        )
        .with_state(system)
}

async fn get_consumer_group(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id, consumer_group_id)): Path<(String, String, u32)>,
) -> Result<Json<ConsumerGroupDetails>, CustomError> {
    let system = system.read().await;
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    let consumer_group = system
        .get_stream(&stream_id)?
        .get_topic(&topic_id)?
        .get_consumer_group(consumer_group_id)?;
    let consumer_group = consumer_group.read().await;
    let consumer_group = mapper::map_consumer_group(&consumer_group).await;
    Ok(Json(consumer_group))
}

async fn get_consumer_groups(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<Json<Vec<ConsumerGroup>>, CustomError> {
    let system = system.read().await;
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    let topic = system.get_stream(&stream_id)?.get_topic(&topic_id)?;
    let consumer_groups = mapper::map_consumer_groups(&topic.get_consumer_groups()).await;
    Ok(Json(consumer_groups))
}

async fn create_consumer_group(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<CreateConsumerGroup>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;
    let mut system = system.write().await;
    system
        .create_consumer_group(
            &command.stream_id,
            &command.topic_id,
            command.consumer_group_id,
        )
        .await?;
    Ok(StatusCode::CREATED)
}

async fn delete_consumer_group(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id, consumer_group_id)): Path<(String, String, u32)>,
) -> Result<StatusCode, CustomError> {
    let mut system = system.write().await;
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    system
        .delete_consumer_group(&stream_id, &topic_id, consumer_group_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
