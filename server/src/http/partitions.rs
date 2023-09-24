use crate::http::error::CustomError;
use crate::http::jwt::Identity;
use crate::http::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use iggy::identifier::Identifier;
use iggy::partitions::create_partitions::CreatePartitions;
use iggy::partitions::delete_partitions::DeletePartitions;
use iggy::validatable::Validatable;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", post(create_partitions).delete(delete_partitions))
        .with_state(state)
}

async fn create_partitions(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<CreatePartitions>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;
    {
        let system = state.system.read().await;
        let stream = system.get_stream(&command.stream_id)?;
        let topic = stream.get_topic(&command.topic_id)?;
        system
            .permissioner
            .create_partitons(identity.user_id, stream.stream_id, topic.topic_id)?;
    }

    let mut system = state.system.write().await;
    system
        .create_partitions(
            &command.stream_id,
            &command.topic_id,
            command.partitions_count,
        )
        .await?;
    Ok(StatusCode::CREATED)
}

async fn delete_partitions(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut query: Query<DeletePartitions>,
) -> Result<StatusCode, CustomError> {
    query.stream_id = Identifier::from_str_value(&stream_id)?;
    query.topic_id = Identifier::from_str_value(&topic_id)?;
    query.validate()?;
    {
        let system = state.system.read().await;
        let stream = system.get_stream(&query.stream_id)?;
        let topic = stream.get_topic(&query.topic_id)?;
        system.permissioner.delete_partitions(
            identity.user_id,
            stream.stream_id,
            topic.topic_id,
        )?;
    }

    let mut system = state.system.write().await;
    system
        .delete_partitions(&query.stream_id, &query.topic_id, query.partitions_count)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
