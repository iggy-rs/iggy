use crate::http::error::CustomError;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use iggy::messages::poll_messages::PollMessages;
use iggy::messages::send_messages::SendMessages;
use iggy::models::offset::Offset;
use iggy::offsets::get_offset::GetOffset;
use iggy::offsets::store_offset::StoreOffset;
use iggy::validatable::Validatable;
use std::sync::Arc;
use streaming::message::Message;
use streaming::polling_consumer::PollingConsumer;
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

    let consumer = PollingConsumer::Consumer(query.consumer_id);
    let system = system.read().await;
    let topic = system.get_stream(stream_id)?.get_topic(topic_id)?;
    let messages = topic
        .get_messages(
            consumer,
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
        trace!("Last offset: {} will be automatically stored for {}, stream: {}, topic: {}, partition: {}", offset, consumer, query.stream_id, query.topic_id, query.partition_id);
        topic
            .store_offset(consumer, query.partition_id, offset)
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
    let topic = system.get_stream(stream_id)?.get_topic(topic_id)?;
    topic
        .append_messages(command.key_kind, command.key_value, messages)
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

    let consumer = PollingConsumer::Consumer(command.consumer_id);
    let system = system.read().await;
    let topic = system.get_stream(stream_id)?.get_topic(topic_id)?;
    topic
        .store_offset(consumer, command.partition_id, command.offset)
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

    let consumer = PollingConsumer::Consumer(query.consumer_id);
    let system = system.read().await;
    let offset = system
        .get_stream(stream_id)?
        .get_topic(topic_id)?
        .get_offset(consumer, query.partition_id)
        .await?;

    Ok(Json(Offset {
        consumer_id: query.consumer_id,
        offset,
    }))
}
