use crate::http::error::CustomError;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, put};
use axum::{Json, Router};
use shared::messages::poll_messages::PollMessages;
use shared::messages::send_messages::SendMessages;
use shared::offsets::store_offset::StoreOffset;
use shared::validatable::Validatable;
use std::sync::Arc;
use streaming::message::Message;
use streaming::system::System;
use streaming::utils::timestamp;
use tokio::sync::RwLock;
use tracing::trace;

pub fn router(system: Arc<RwLock<System>>) -> Router {
    Router::new()
        .route("/", get(poll_messages).post(send_messages))
        .route("/offsets", put(store_offset))
        .with_state(system)
}

async fn poll_messages(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(u32, u32)>,
    mut query: Query<PollMessages>,
) -> Result<Json<Vec<sdk::message::Message>>, CustomError> {
    query.stream_id = stream_id;
    query.topic_id = topic_id;
    query.validate()?;

    let system = system.read().await;
    let stream = system.get_stream(stream_id)?;
    let messages = stream
        .get_messages(
            query.consumer_id,
            topic_id,
            query.partition_id,
            query.kind,
            query.value,
            query.count,
        )
        .await?
        .iter()
        .map(|message| sdk::message::Message {
            offset: message.offset,
            timestamp: message.timestamp,
            id: message.id,
            length: message.length,
            payload: message.payload.clone(),
        })
        .collect::<Vec<sdk::message::Message>>();

    if messages.is_empty() {
        return Ok(Json(messages));
    }

    if query.auto_commit {
        let offset = messages.last().unwrap().offset;
        trace!("Last offset: {} will be automatically stored for consumer: {}, stream: {}, topic: {}, partition: {}", offset, query.consumer_id, query.stream_id, query.topic_id, query.partition_id);
        stream
            .store_offset(
                query.consumer_id,
                query.topic_id,
                query.partition_id,
                offset,
            )
            .await?;
    }

    Ok(Json(messages))
}

async fn send_messages(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(u32, u32)>,
    Json(mut command): Json<SendMessages>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = stream_id;
    command.topic_id = topic_id;
    command.messages_count = command.messages.len() as u32;
    command.validate()?;

    let mut messages = Vec::with_capacity(command.messages_count as usize);
    for message in command.messages {
        let timestamp = timestamp::get();
        messages.push(Message::empty(timestamp, message.id, message.payload));
    }

    let system = system.read().await;
    let stream = system.get_stream(stream_id)?;
    stream
        .append_messages(
            command.topic_id,
            command.key_kind,
            command.key_value,
            messages,
            false,
        )
        .await?;
    Ok(StatusCode::CREATED)
}

async fn store_offset(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(u32, u32)>,
    mut command: Json<StoreOffset>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = stream_id;
    command.topic_id = topic_id;
    command.validate()?;

    let system = system.read().await;
    let stream = system.get_stream(stream_id)?;
    stream
        .store_offset(
            command.consumer_id,
            command.topic_id,
            command.partition_id,
            command.offset,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
