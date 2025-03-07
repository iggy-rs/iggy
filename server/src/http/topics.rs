use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::mapper;
use crate::http::shared::AppState;
use crate::http::COMPONENT;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Extension, Json, Router};
use error_set::ErrContext;
use iggy::identifier::Identifier;
use iggy::models::topic::{Topic, TopicDetails};
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::delete_topic::DeleteTopic;
use iggy::topics::purge_topic::PurgeTopic;
use iggy::topics::update_topic::UpdateTopic;
use iggy::validatable::Validatable;
use std::sync::Arc;
use tracing::instrument;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/streams/{stream_id}/topics",
            get(get_topics).post(create_topic),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}",
            get(get_topic).put(update_topic).delete(delete_topic),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/purge",
            delete(purge_topic),
        )
        .with_state(state)
}

async fn get_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<Json<TopicDetails>, CustomError> {
    let system = state.system.read().await;
    let identity_stream_id = Identifier::from_str_value(&stream_id)?;
    let identity_topic_id = Identifier::from_str_value(&topic_id)?;
    let Ok(topic) = system.try_find_topic(
        &Session::stateless(identity.user_id, identity.ip_address),
        &identity_stream_id,
        &identity_topic_id,
    ) else {
        return Err(CustomError::ResourceNotFound);
    };
    let Some(topic) = topic else {
        return Err(CustomError::ResourceNotFound);
    };

    let topic = mapper::map_topic(topic).await;
    Ok(Json(topic))
}

async fn get_topics(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<Json<Vec<Topic>>, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let system = state.system.read().await;
    let topics = system
        .find_topics(
            &Session::stateless(identity.user_id, identity.ip_address),
            &stream_id,
        )
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to find topics for stream with ID: {}",
                stream_id
            )
        })?;
    let topics = mapper::map_topics(&topics);
    Ok(Json(topics))
}

#[instrument(skip_all, name = "trace_create_topic", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id))]
async fn create_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
    Json(mut command): Json<CreateTopic>,
) -> Result<Json<TopicDetails>, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.validate()?;

    let mut system = state.system.write().await;
    let topic = system
        .create_topic(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.stream_id,
            command.topic_id,
            &command.name,
            command.partitions_count,
            command.message_expiry,
            command.compression_algorithm,
            command.max_topic_size,
            command.replication_factor,
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to create topic, stream ID: {}",
                stream_id
            )
        })?;
    command.message_expiry = topic.message_expiry;
    command.max_topic_size = topic.max_topic_size;
    let response = Json(mapper::map_topic(topic).await);

    let system = system.downgrade();
    system
        .state
        .apply(identity.user_id, &EntryCommand::CreateTopic(command))
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply create topic, stream ID: {}",
                stream_id
            )
        })?;
    Ok(response)
}

#[instrument(skip_all, name = "trace_update_topic", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id))]
async fn update_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<UpdateTopic>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;

    let mut system = state.system.write().await;
    let topic = system
            .update_topic(
                &Session::stateless(identity.user_id, identity.ip_address),
                &command.stream_id,
                &command.topic_id,
                &command.name,
                command.message_expiry,
                command.compression_algorithm,
                command.max_topic_size,
                command.replication_factor,
            )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to update topic, stream ID: {}, topic ID: {}",
                    stream_id, topic_id
                )
            })?;
    command.message_expiry = topic.message_expiry;
    command.max_topic_size = topic.max_topic_size;

    let system = system.downgrade();
    system
        .state
        .apply(identity.user_id, &EntryCommand::UpdateTopic(command))
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply update topic, stream ID: {}, topic ID: {}",
                stream_id, topic_id
            )
        })?;
    Ok(StatusCode::NO_CONTENT)
}

#[instrument(skip_all, name = "trace_delete_topic", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id))]
async fn delete_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<StatusCode, CustomError> {
    let identifier_stream_id = Identifier::from_str_value(&stream_id)?;
    let identifier_topic_id = Identifier::from_str_value(&topic_id)?;

    let mut system = state.system.write().await;
    system
            .delete_topic(
                &Session::stateless(identity.user_id, identity.ip_address),
                &identifier_stream_id,
                &identifier_topic_id,
            )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to delete topic with ID: {topic_id} in stream with ID: {stream_id}",
                )
            })?;

    let system = system.downgrade();
    system
        .state
        .apply(
            identity.user_id,
            &EntryCommand::DeleteTopic(DeleteTopic {
                stream_id: identifier_stream_id,
                topic_id: identifier_topic_id,
            }),
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply delete topic, stream ID: {}, topic ID: {}",
                stream_id, topic_id
            )
        })?;
    Ok(StatusCode::NO_CONTENT)
}

#[instrument(skip_all, name = "trace_purge_topic", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id))]
async fn purge_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<StatusCode, CustomError> {
    let identifier_stream_id = Identifier::from_str_value(&stream_id)?;
    let identifier_topic_id = Identifier::from_str_value(&topic_id)?;
    let system = state.system.read().await;
    system
        .purge_topic(
            &Session::stateless(identity.user_id, identity.ip_address),
            &identifier_stream_id,
            &identifier_topic_id,
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to purge topic, stream ID: {}, topic ID: {}",
                stream_id, topic_id
            )
        })?;
    system
        .state
        .apply(
            identity.user_id,
            &EntryCommand::PurgeTopic(PurgeTopic {
                stream_id: identifier_stream_id,
                topic_id: identifier_topic_id,
            }),
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply purge topic, stream ID: {}, topic ID: {}",
                stream_id, topic_id
            )
        })?;
    Ok(StatusCode::NO_CONTENT)
}
