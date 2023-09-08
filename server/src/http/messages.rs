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
use streaming::systems::messages::PollMessagesArgs;
use streaming::systems::system::System;
use streaming::users::user_context::UserContext;
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

    // TODO: Resolve user ID from JWT
    let user_context = UserContext::from_user(1);
    let partition_id = query.partition_id.unwrap_or(0);
    let consumer = PollingConsumer::Consumer(query.consumer.id, partition_id);
    let system = system.read().await;
    let polled_messages = system
        .poll_messages(
            &user_context,
            consumer,
            &query.stream_id,
            &query.topic_id,
            PollMessagesArgs {
                strategy: query.strategy,
                count: query.count,
                auto_commit: query.auto_commit,
            },
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
    // TODO: Resolve user ID from JWT
    let user_context = UserContext::from_user(1);
    let system = system.read().await;
    system
        .append_messages(
            &user_context,
            &command.stream_id,
            &command.topic_id,
            &command.partitioning,
            &command.messages,
        )
        .await?;
    Ok(StatusCode::CREATED)
}
