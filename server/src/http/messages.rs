use crate::http::error::CustomError;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use sdk::messages::poll_messages::PollMessages;
use sdk::messages::send_messages::SendMessages;
use sdk::offset::Offset;
use sdk::offsets::get_offset::GetOffset;
use sdk::offsets::store_offset::StoreOffset;
use sdk::validatable::Validatable;
use std::sync::Arc;
use streaming::message::Message;
use streaming::system::System;
use streaming::utils::{checksum, timestamp};
use tokio::sync::RwLock;
use tracing::trace;

pub fn router(system: Arc<RwLock<System>>) -> Router {
    Router::new()
        .route("/", get(poll_messages).post(send_messages))
        .route("/offsets", get(get_offset).put(store_offset))
        .with_state(system)
}

async fn poll_messages(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(u32, u32)>,
    mut query: Query<PollMessages>,
) -> Result<Json<Vec<Arc<Message>>>, CustomError> {
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
        .await?;

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
        let checksum = checksum::get(&message.payload);
        messages.push(Message::empty(
            timestamp,
            message.id,
            message.payload,
            checksum,
        ));
    }

    let system = system.read().await;
    let stream = system.get_stream(stream_id)?;
    let storage = system.storage.segment.clone();
    stream
        .append_messages(
            command.topic_id,
            command.key_kind,
            command.key_value,
            messages,
            storage,
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

async fn get_offset(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(u32, u32)>,
    mut query: Query<GetOffset>,
) -> Result<Json<Offset>, CustomError> {
    query.stream_id = stream_id;
    query.topic_id = topic_id;
    query.validate()?;

    let system = system.read().await;
    let stream = system.get_stream(stream_id)?;
    let offset = stream
        .get_offset(query.consumer_id, query.topic_id, query.partition_id)
        .await?;

    Ok(Json(Offset {
        consumer_id: query.consumer_id,
        offset,
    }))
}
