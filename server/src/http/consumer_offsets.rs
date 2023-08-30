use crate::http::error::CustomError;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use iggy::identifier::Identifier;
use iggy::models::consumer_offset_info::ConsumerOffsetInfo;
use iggy::validatable::Validatable;
use std::sync::Arc;
use streaming::polling_consumer::PollingConsumer;
use streaming::systems::system::System;
use tokio::sync::RwLock;

pub fn router(system: Arc<RwLock<System>>) -> Router {
    Router::new()
        .route("/", get(get_consumer_offset).put(store_consumer_offset))
        .with_state(system)
}

async fn get_consumer_offset(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut query: Query<GetConsumerOffset>,
) -> Result<Json<ConsumerOffsetInfo>, CustomError> {
    query.stream_id = Identifier::from_str_value(&stream_id)?;
    query.topic_id = Identifier::from_str_value(&topic_id)?;
    query.validate()?;

    let consumer = PollingConsumer::Consumer(query.consumer.id, query.partition_id.unwrap_or(0));
    let system = system.read().await;
    let stream = system.get_stream(&query.stream_id)?;
    let topic = stream.get_topic(&query.topic_id)?;
    let offset = topic.get_consumer_offset(consumer).await?;

    Ok(Json(offset))
}

async fn store_consumer_offset(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut command: Json<StoreConsumerOffset>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;

    let consumer =
        PollingConsumer::Consumer(command.consumer.id, command.partition_id.unwrap_or(0));
    let system = system.read().await;
    let stream = system.get_stream(&command.stream_id)?;
    let topic = stream.get_topic(&command.topic_id)?;
    topic
        .store_consumer_offset(consumer, command.offset)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
