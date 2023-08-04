use crate::http::error::CustomError;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use iggy::error::Error;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollMessages;
use iggy::messages::send_messages::SendMessages;
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
        .with_state(system)
}

async fn poll_messages(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut query: Query<PollMessages>,
) -> Result<Json<Vec<Arc<Message>>>, CustomError> {
    query.stream_id = Identifier::from_str_value(&stream_id)?;
    query.topic_id = Identifier::from_str_value(&topic_id)?;
    query.validate()?;

    let consumer = PollingConsumer::Consumer(query.consumer.id);
    let system = system.read().await;
    let stream = system.get_stream(&query.stream_id)?;
    let topic = stream.get_topic(&query.topic_id)?;
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
            .store_consumer_offset(consumer, query.partition_id, offset)
            .await?;
    }

    Ok(Json(messages))
}

async fn send_messages(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<SendMessages>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.key.length = command.key.value.len() as u8;
    command.validate()?;

    let mut messages = Vec::with_capacity(command.messages.len());
    for message in &command.messages {
        messages.push(Message::from_message(message));
    }

    let system = system.read().await;
    let stream = system.get_stream(&command.stream_id)?;
    let topic = stream.get_topic(&command.topic_id)?;
    topic.append_messages(&command.key, messages).await?;
    Ok(StatusCode::CREATED)
}
