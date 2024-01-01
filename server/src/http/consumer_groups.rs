use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::mapper;
use crate::http::shared::AppState;
use crate::streaming::session::Session;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::identifier::Identifier;
use iggy::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use iggy::validatable::Validatable;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/streams/:stream_id/topics/:topic_id/consumer-groups",
            get(get_consumer_groups).post(create_consumer_group),
        )
        .route(
            "/streams/:stream_id/topics/:topic_id/consumer-groups/:consumer_group_id",
            get(get_consumer_group).delete(delete_consumer_group),
        )
        .with_state(state)
}

async fn get_consumer_group(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id, consumer_group_id)): Path<(String, String, String)>,
) -> Result<Json<ConsumerGroupDetails>, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    let consumer_group_id = Identifier::from_str_value(&consumer_group_id)?;
    let system = state.system.read();
    let consumer_group = system.get_consumer_group(
        &Session::stateless(identity.user_id, identity.ip_address),
        &stream_id,
        &topic_id,
        &consumer_group_id,
    )?;
    let consumer_group = consumer_group.read().await;
    let consumer_group = mapper::map_consumer_group(&consumer_group).await;
    Ok(Json(consumer_group))
}

async fn get_consumer_groups(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<Json<Vec<ConsumerGroup>>, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    let system = state.system.read();
    let consumer_groups = system.get_consumer_groups(
        &Session::stateless(identity.user_id, identity.ip_address),
        &stream_id,
        &topic_id,
    )?;
    let consumer_groups = mapper::map_consumer_groups(&consumer_groups).await;
    Ok(Json(consumer_groups))
}

async fn create_consumer_group(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<CreateConsumerGroup>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;
    let mut system = state.system.write();
    system
        .create_consumer_group(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.stream_id,
            &command.topic_id,
            command.consumer_group_id,
            &command.name,
        )
        .await?;
    Ok(StatusCode::CREATED)
}

async fn delete_consumer_group(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id, consumer_group_id)): Path<(String, String, String)>,
) -> Result<StatusCode, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    let consumer_group_id = Identifier::from_str_value(&consumer_group_id)?;
    let mut system = state.system.write();
    system
        .delete_consumer_group(
            &Session::stateless(identity.user_id, identity.ip_address),
            &stream_id,
            &topic_id,
            &consumer_group_id,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
