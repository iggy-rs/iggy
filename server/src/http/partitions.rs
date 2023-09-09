use crate::http::error::CustomError;
use crate::streaming::systems::system::System;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use iggy::identifier::Identifier;
use iggy::partitions::create_partitions::CreatePartitions;
use iggy::partitions::delete_partitions::DeletePartitions;
use iggy::validatable::Validatable;
use std::sync::Arc;
use tokio::sync::RwLock;

pub fn router(system: Arc<RwLock<System>>) -> Router {
    Router::new()
        .route("/", post(create_partitions).delete(delete_partitions))
        .with_state(system)
}

async fn create_partitions(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<CreatePartitions>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;
    let mut system = system.write().await;
    let topic = system
        .get_stream_mut(&command.stream_id)?
        .get_topic_mut(&command.topic_id)?;
    topic
        .add_persisted_partitions(command.partitions_count)
        .await?;
    topic.reassign_consumer_groups().await;
    Ok(StatusCode::CREATED)
}

async fn delete_partitions(
    State(system): State<Arc<RwLock<System>>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut query: Query<DeletePartitions>,
) -> Result<StatusCode, CustomError> {
    query.stream_id = Identifier::from_str_value(&stream_id)?;
    query.topic_id = Identifier::from_str_value(&topic_id)?;
    query.validate()?;
    let mut system = system.write().await;
    let topic = system
        .get_stream_mut(&query.stream_id)?
        .get_topic_mut(&query.topic_id)?;
    topic
        .delete_persisted_partitions(query.partitions_count)
        .await?;
    topic.reassign_consumer_groups().await;
    Ok(StatusCode::NO_CONTENT)
}
