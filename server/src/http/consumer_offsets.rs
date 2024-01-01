use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::shared::AppState;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::session::Session;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use iggy::identifier::Identifier;
use iggy::models::consumer_offset_info::ConsumerOffsetInfo;
use iggy::validatable::Validatable;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/streams/:stream_id/topics/:topic_id/consumer-offsets",
            get(get_consumer_offset).put(store_consumer_offset),
        )
        .with_state(state)
}

async fn get_consumer_offset(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut query: Query<GetConsumerOffset>,
) -> Result<Json<ConsumerOffsetInfo>, CustomError> {
    query.stream_id = Identifier::from_str_value(&stream_id)?;
    query.topic_id = Identifier::from_str_value(&topic_id)?;
    query.validate()?;
    let consumer_id = PollingConsumer::resolve_consumer_id(&query.consumer.id);
    let consumer = PollingConsumer::Consumer(consumer_id, query.partition_id.unwrap_or(0));
    let system = state.system.read();
    let offset = system
        .get_consumer_offset(
            &Session::stateless(identity.user_id, identity.ip_address),
            consumer,
            &query.stream_id,
            &query.topic_id,
        )
        .await?;
    Ok(Json(offset))
}

async fn store_consumer_offset(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut command: Json<StoreConsumerOffset>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;
    let consumer_id = PollingConsumer::resolve_consumer_id(&command.consumer.id);
    let consumer = PollingConsumer::Consumer(consumer_id, command.partition_id.unwrap_or(0));
    let system = state.system.read();
    system
        .store_consumer_offset(
            &Session::stateless(identity.user_id, identity.ip_address),
            consumer,
            &command.stream_id,
            &command.topic_id,
            command.offset,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
