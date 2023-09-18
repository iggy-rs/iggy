use crate::http::error::CustomError;
use crate::http::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use iggy::identifier::Identifier;
use iggy::partitions::create_partitions::CreatePartitions;
use iggy::partitions::delete_partitions::DeletePartitions;
use iggy::validatable::Validatable;
use std::sync::Arc;

use super::auth;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", post(create_partitions).delete(delete_partitions))
        .with_state(state)
}

async fn create_partitions(
    State(state): State<Arc<AppState>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<CreatePartitions>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;
    let user_id = auth::resolve_user_id();
    {
        let system = state.system.read().await;
        let stream = system.get_stream(&command.stream_id)?;
        let topic = stream.get_topic(&command.topic_id)?;
        system
            .permissioner
            .create_partitons(user_id, stream.stream_id, topic.topic_id)?;
    }

    let mut system = state.system.write().await;
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
    State(state): State<Arc<AppState>>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut query: Query<DeletePartitions>,
) -> Result<StatusCode, CustomError> {
    query.stream_id = Identifier::from_str_value(&stream_id)?;
    query.topic_id = Identifier::from_str_value(&topic_id)?;
    query.validate()?;
    let user_id = auth::resolve_user_id();
    {
        let system = state.system.read().await;
        let stream = system.get_stream(&query.stream_id)?;
        let topic = stream.get_topic(&query.topic_id)?;
        system
            .permissioner
            .delete_partitions(user_id, stream.stream_id, topic.topic_id)?;
    }

    let mut system = state.system.write().await;
    let topic = system
        .get_stream_mut(&query.stream_id)?
        .get_topic_mut(&query.topic_id)?;
    topic
        .delete_persisted_partitions(query.partitions_count)
        .await?;
    topic.reassign_consumer_groups().await;
    Ok(StatusCode::NO_CONTENT)
}
