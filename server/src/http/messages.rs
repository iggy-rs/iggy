use crate::http::error::CustomError;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use iggy::error::Error;
use iggy::identifier::{IdKind, Identifier};
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
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut query: Query<PollMessages>,
) -> Result<Json<Vec<Arc<Message>>>, CustomError> {
    query.stream_id = match stream_id.parse::<u32>() {
        Ok(id) => Identifier::numeric(id).unwrap(),
        Err(_) => Identifier::string(&stream_id).unwrap(),
    };
    query.topic_id = match topic_id.parse::<u32>() {
        Ok(id) => Identifier::numeric(id).unwrap(),
        Err(_) => Identifier::string(&topic_id).unwrap(),
    };
    query.validate()?;

    let consumer = PollingConsumer::Consumer(query.consumer_id);
    let system = system.read().await;
    let stream = match query.stream_id.kind {
        IdKind::Numeric => system.get_stream_by_id(query.stream_id.as_u32().unwrap())?,
        IdKind::String => system.get_stream_by_name(&query.stream_id.as_string().unwrap())?,
    };
    let topic = match query.topic_id.kind {
        IdKind::Numeric => stream.get_topic_by_id(query.topic_id.as_u32().unwrap())?,
        IdKind::String => stream.get_topic_by_name(&query.topic_id.as_string().unwrap())?,
    };
    if !topic.has_partitions() {
        return Err(CustomError::from(Error::NoPartitions(
            topic.id,
            topic.stream_id,
        )));
    }

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
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<SendMessages>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = match stream_id.parse::<u32>() {
        Ok(id) => Identifier::numeric(id).unwrap(),
        Err(_) => Identifier::string(&stream_id).unwrap(),
    };
    command.topic_id = match topic_id.parse::<u32>() {
        Ok(id) => Identifier::numeric(id).unwrap(),
        Err(_) => Identifier::string(&topic_id).unwrap(),
    };
    command.key.length = command.key.value.len() as u8;
    command.messages_count = command.messages.len() as u32;
    command.validate()?;

    let mut messages = Vec::with_capacity(command.messages_count as usize);
    for message in &command.messages {
        messages.push(Message::from_message(message));
    }

    let system = system.read().await;
    let stream = match command.stream_id.kind {
        IdKind::Numeric => system.get_stream_by_id(command.stream_id.as_u32().unwrap())?,
        IdKind::String => system.get_stream_by_name(&command.stream_id.as_string().unwrap())?,
    };
    let topic = match command.topic_id.kind {
        IdKind::Numeric => stream.get_topic_by_id(command.topic_id.as_u32().unwrap())?,
        IdKind::String => stream.get_topic_by_name(&command.topic_id.as_string().unwrap())?,
    };
    topic.append_messages(&command.key, messages).await?;
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
    let topic = system
        .get_stream_by_id(stream_id)?
        .get_topic_by_id(topic_id)?;
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
        .get_stream_by_id(stream_id)?
        .get_topic_by_id(topic_id)?
        .get_offset(consumer, query.partition_id)
        .await?;

    Ok(Json(Offset {
        consumer_id: query.consumer_id,
        offset,
    }))
}
