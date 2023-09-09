use crate::http::auth;
use crate::http::error::CustomError;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollMessages;
use iggy::messages::send_messages::SendMessages;
use iggy::validatable::Validatable;
use std::sync::Arc;
use streaming::polling_consumer::PollingConsumer;
use streaming::systems::system::System;
use tokio::sync::RwLock;

pub fn router(system: Arc<RwLock<System>>) -> Router {
    Router::new()
        .route("/", get(poll_messages).post(send_messages))
        .with_state(system)
}

async fn poll_messages(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut query: Query<PollMessages>,
) -> Result<Json<streaming::models::messages::PolledMessages>, CustomError> {
    query.stream_id = Identifier::from_str_value(&stream_id)?;
    query.topic_id = Identifier::from_str_value(&topic_id)?;
    query.validate()?;

    let user_id = auth::resolve_user_id();
    let partition_id = query.partition_id.unwrap_or(0);
    let consumer = PollingConsumer::Consumer(query.consumer.id, partition_id);
    let system = system.read().await;
    let stream = system.get_stream(&query.stream_id)?;
    let topic = stream.get_topic(&query.topic_id)?;
    system
        .permissioner
        .poll_messages(user_id, stream.stream_id, topic.topic_id)?;

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
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<SendMessages>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.partitioning.length = command.partitioning.value.len() as u8;
    command.validate()?;

    let user_id = auth::resolve_user_id();
    let system = system.read().await;
    let stream = system.get_stream(&command.stream_id)?;
    let topic = stream.get_topic(&command.topic_id)?;
    system
        .permissioner
        .append_messages(user_id, stream.stream_id, topic.topic_id)?;
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
