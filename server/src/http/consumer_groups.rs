use crate::http::error::CustomError;
use crate::http::mapper;
use crate::http::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::identifier::Identifier;
use iggy::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use iggy::validatable::Validatable;
use std::sync::Arc;

use super::auth;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(get_consumer_groups).post(create_consumer_group))
        .route(
            "/:consumer_group_id",
            get(get_consumer_group).delete(delete_consumer_group),
        )
        .with_state(state)
}

async fn get_consumer_group(
    State(state): State<Arc<AppState>>,
    Path((stream_id, topic_id, consumer_group_id)): Path<(String, String, u32)>,
) -> Result<Json<ConsumerGroupDetails>, CustomError> {
    let user_id = auth::resolve_user_id();
    let system = state.system.read().await;
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    let stream = system.get_stream(&stream_id)?;
    let topic = stream.get_topic(&topic_id)?;
    system
        .permissioner
        .get_consumer_group(user_id, stream.stream_id, topic.topic_id)?;
    let consumer_group = topic.get_consumer_group(consumer_group_id)?;
    let consumer_group = consumer_group.read().await;
    let consumer_group = mapper::map_consumer_group(&consumer_group).await;
    Ok(Json(consumer_group))
}

async fn get_consumer_groups(
    State(state): State<Arc<AppState>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<Json<Vec<ConsumerGroup>>, CustomError> {
    let user_id = auth::resolve_user_id();
    let system = state.system.read().await;
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    let stream = system.get_stream(&stream_id)?;
    let topic = stream.get_topic(&topic_id)?;
    system
        .permissioner
        .get_consumer_groups(user_id, stream.stream_id, topic.topic_id)?;
    let consumer_groups = mapper::map_consumer_groups(&topic.get_consumer_groups()).await;
    Ok(Json(consumer_groups))
}

async fn create_consumer_group(
    State(state): State<Arc<AppState>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<CreateConsumerGroup>,
) -> Result<StatusCode, CustomError> {
    let user_id = auth::resolve_user_id();
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;
    {
        let system = state.system.read().await;
        let stream = system.get_stream(&command.stream_id)?;
        let topic = stream.get_topic(&command.topic_id)?;
        system
            .permissioner
            .create_consumer_group(user_id, stream.stream_id, topic.topic_id)?;
    }

    let mut system = state.system.write().await;
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
    State(state): State<Arc<AppState>>,
    Path((stream_id, topic_id, consumer_group_id)): Path<(String, String, u32)>,
) -> Result<StatusCode, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    let user_id = auth::resolve_user_id();
    {
        let system = state.system.read().await;
        let stream = system.get_stream(&stream_id)?;
        let topic = stream.get_topic(&topic_id)?;
        system
            .permissioner
            .delete_consumer_group(user_id, stream.stream_id, topic.topic_id)?;
    }

    let mut system = state.system.write().await;
    system
        .delete_consumer_group(&stream_id, &topic_id, consumer_group_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
