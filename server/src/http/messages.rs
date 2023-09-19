use crate::http::error::CustomError;
use crate::http::jwt::Identity;
use crate::http::state::AppState;
use crate::streaming;
use crate::streaming::polling_consumer::PollingConsumer;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollMessages;
use iggy::messages::send_messages::SendMessages;
use iggy::validatable::Validatable;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(poll_messages).post(send_messages))
        .with_state(state)
}

async fn poll_messages(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut query: Query<PollMessages>,
) -> Result<Json<streaming::models::messages::PolledMessages>, CustomError> {
    query.stream_id = Identifier::from_str_value(&stream_id)?;
    query.topic_id = Identifier::from_str_value(&topic_id)?;
    query.validate()?;

    let partition_id = query.partition_id.unwrap_or(0);
    let consumer = PollingConsumer::Consumer(query.consumer.id, partition_id);
    let system = state.system.read().await;
    let stream = system.get_stream(&query.stream_id)?;
    let topic = stream.get_topic(&query.topic_id)?;
    system
        .permissioner
        .poll_messages(identity.user_id, stream.stream_id, topic.topic_id)?;

    let polled_messages = system
        .poll_messages(
            consumer,
            &query.stream_id,
            &query.topic_id,
            query.strategy,
            query.count,
            query.auto_commit,
        )
        .await?;
    Ok(Json(polled_messages))
}

async fn send_messages(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<SendMessages>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.partitioning.length = command.partitioning.value.len() as u8;
    command.validate()?;

    let system = state.system.read().await;
    let stream = system.get_stream(&command.stream_id)?;
    let topic = stream.get_topic(&command.topic_id)?;
    system
        .permissioner
        .append_messages(identity.user_id, stream.stream_id, topic.topic_id)?;
    system
        .append_messages(
            &command.stream_id,
            &command.topic_id,
            &command.partitioning,
            &command.messages,
        )
        .await?;
    Ok(StatusCode::CREATED)
}
