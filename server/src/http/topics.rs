use crate::http::error::CustomError;
use crate::http::mapper;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use iggy::identifier::Identifier;
use iggy::models::topic::{Topic, TopicDetails};
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::update_topic::UpdateTopic;
use iggy::validatable::Validatable;
use std::sync::Arc;
use streaming::systems::system::System;
use tokio::sync::RwLock;

pub fn router(system: Arc<RwLock<System>>) -> Router {
    Router::new()
        .route("/", get(get_topics).post(create_topic))
        .route(
            "/:topic_id",
            get(get_topic).put(update_topic).delete(delete_topic),
        )
        .with_state(system)
}

async fn get_topic(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<Json<TopicDetails>, CustomError> {
    let system = system.read().await;
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    let topic = system.get_stream(&stream_id)?.get_topic(&topic_id)?;
    let topic = mapper::map_topic(topic).await;
    Ok(Json(topic))
}

async fn get_topics(
    State(system): State<Arc<RwLock<System>>>,
    Path(stream_id): Path<String>,
) -> Result<Json<Vec<Topic>>, CustomError> {
    let system = system.read().await;
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topics = system.get_stream(&stream_id)?.get_topics();
    let topics = mapper::map_topics(&topics).await;
    Ok(Json(topics))
}

async fn create_topic(
    State(system): State<Arc<RwLock<System>>>,
    Path(stream_id): Path<String>,
    Json(mut command): Json<CreateTopic>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.validate()?;
    let mut system = system.write().await;
    system
        .get_stream_mut(&command.stream_id)?
        .create_topic(
            command.topic_id,
            &command.name,
            command.partitions_count,
            command.message_expiry,
        )
        .await?;
    Ok(StatusCode::CREATED)
}

async fn update_topic(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<UpdateTopic>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;
    let mut system = system.write().await;
    system
        .get_stream_mut(&command.stream_id)?
        .update_topic(&command.topic_id, &command.name, command.message_expiry)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_topic(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<StatusCode, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    let mut system = system.write().await;
    system.delete_topic(&stream_id, &topic_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
