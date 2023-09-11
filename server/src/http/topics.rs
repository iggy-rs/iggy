use crate::http::error::CustomError;
use crate::http::{auth, mapper};
use crate::streaming::systems::system::System;
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
    let user_id = auth::resolve_user_id();
    let system = system.read().await;
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    let stream = system.get_stream(&stream_id)?;
    let topic = stream.get_topic(&topic_id)?;
    system
        .permissioner
        .get_topic(user_id, stream.stream_id, topic.topic_id)?;
    let topic = mapper::map_topic(topic).await;
    Ok(Json(topic))
}

async fn get_topics(
    State(system): State<Arc<RwLock<System>>>,
    Path(stream_id): Path<String>,
) -> Result<Json<Vec<Topic>>, CustomError> {
    let user_id = auth::resolve_user_id();
    let system = system.read().await;
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let stream = system.get_stream(&stream_id)?;
    system.permissioner.get_topics(user_id, stream.stream_id)?;
    let topics = stream.get_topics();
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
    let user_id = auth::resolve_user_id();
    {
        let system = system.read().await;
        let stream = system.get_stream(&command.stream_id)?;
        system
            .permissioner
            .create_topic(user_id, stream.stream_id)?;
    }

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
    let user_id = auth::resolve_user_id();
    {
        let system = system.read().await;
        let stream = system.get_stream(&command.stream_id)?;
        let topic = stream.get_topic(&command.topic_id)?;
        system
            .permissioner
            .update_topic(user_id, stream.stream_id, topic.topic_id)?;
    }

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
    let user_id = auth::resolve_user_id();
    {
        let system = system.read().await;
        let stream = system.get_stream(&stream_id)?;
        let topic = stream.get_topic(&topic_id)?;
        system
            .permissioner
            .delete_topic(user_id, stream.stream_id, topic.topic_id)?;
    }

    let mut system = system.write().await;
    system.delete_topic(&stream_id, &topic_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
