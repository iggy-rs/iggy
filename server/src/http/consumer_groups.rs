use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::mapper;
use crate::http::shared::AppState;
use crate::http::COMPONENT;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use error_set::ErrContext;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::identifier::Identifier;
use iggy::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use iggy::validatable::Validatable;
use std::sync::Arc;
use tracing::instrument;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/streams/{stream_id}/topics/{topic_id}/consumer-groups",
            get(get_consumer_groups).post(create_consumer_group),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/consumer-groups/{group_id}",
            get(get_consumer_group).delete(delete_consumer_group),
        )
        .with_state(state)
}

async fn get_consumer_group(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id, group_id)): Path<(String, String, String)>,
) -> Result<Json<ConsumerGroupDetails>, CustomError> {
    let identifier_stream_id = Identifier::from_str_value(&stream_id)?;
    let identifier_topic_id = Identifier::from_str_value(&topic_id)?;
    let identifier_group_id = Identifier::from_str_value(&group_id)?;
    let system = state.system.read().await;
    let Ok(consumer_group) = system.get_consumer_group(
        &Session::stateless(identity.user_id, identity.ip_address),
        &identifier_stream_id,
        &identifier_topic_id,
        &identifier_group_id,
    ) else {
        return Err(CustomError::ResourceNotFound);
    };
    let Some(consumer_group) = consumer_group else {
        return Err(CustomError::ResourceNotFound);
    };

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
    let system = state.system.read().await;
    let consumer_groups = system.get_consumer_groups(
        &Session::stateless(identity.user_id, identity.ip_address),
        &stream_id,
        &topic_id,
    )?;
    let consumer_groups = mapper::map_consumer_groups(&consumer_groups).await;
    Ok(Json(consumer_groups))
}

#[instrument(skip_all, name = "trace_create_consumer_group", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id))]
async fn create_consumer_group(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<CreateConsumerGroup>,
) -> Result<(StatusCode, Json<ConsumerGroupDetails>), CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;
    let mut system = state.system.write().await;
    let consumer_group = system
            .create_consumer_group(
                &Session::stateless(identity.user_id, identity.ip_address),
                &command.stream_id,
                &command.topic_id,
                command.group_id,
                &command.name,
            )
            .await
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to create consumer group, stream ID: {}, topic ID: {}, group ID: {:?}", stream_id, topic_id, command.group_id))?;
    let consumer_group = consumer_group.read().await;
    let consumer_group_details = mapper::map_consumer_group(&consumer_group).await;
    drop(consumer_group);

    let system = system.downgrade();
    system
        .state
        .apply(
            identity.user_id,
            &EntryCommand::CreateConsumerGroup(command),
        )
        .await?;

    Ok((StatusCode::CREATED, Json(consumer_group_details)))
}

#[instrument(skip_all, name = "trace_delete_consumer_group", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id, iggy_group_id = group_id))]
async fn delete_consumer_group(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id, group_id)): Path<(String, String, String)>,
) -> Result<StatusCode, CustomError> {
    let identifier_stream_id = Identifier::from_str_value(&stream_id)?;
    let identifier_topic_id = Identifier::from_str_value(&topic_id)?;
    let identifier_group_id = Identifier::from_str_value(&group_id)?;

    let mut system = state.system.write().await;
    system
            .delete_consumer_group(
                &Session::stateless(identity.user_id, identity.ip_address),
                &identifier_stream_id,
                &identifier_topic_id,
                &identifier_group_id,
            )
            .await
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to delete consumer group with ID: {group_id} for topic with ID: {topic_id} in stream with ID: {stream_id}"))?;

    let system = system.downgrade();
    system
        .state
        .apply(
            identity.user_id,
            &EntryCommand::DeleteConsumerGroup(DeleteConsumerGroup {
                stream_id: identifier_stream_id,
                topic_id: identifier_topic_id,
                group_id: identifier_group_id,
            }),
        )
        .await?;

    Ok(StatusCode::NO_CONTENT)
}
