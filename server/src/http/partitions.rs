use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::shared::AppState;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use iggy::identifier::Identifier;
use iggy::partitions::create_partitions::CreatePartitions;
use iggy::partitions::delete_partitions::DeletePartitions;
use iggy::validatable::Validatable;
use std::sync::Arc;
use tracing::instrument;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/streams/:stream_id/topics/:topic_id/partitions",
            post(create_partitions).delete(delete_partitions),
        )
        .with_state(state)
}

#[instrument(skip_all, fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id))]
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
        let mut system = state.system.write().await;
        system
            .create_partitions(
                &Session::stateless(identity.user_id, identity.ip_address),
                &command.stream_id,
                &command.topic_id,
                command.partitions_count,
            )
            .await?;
    }

    let system = state.system.read().await;
    system
        .state
        .apply(identity.user_id, EntryCommand::CreatePartitions(command))
        .await?;
    Ok(StatusCode::CREATED)
}

#[instrument(skip_all, fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id))]
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
        let mut system = state.system.write().await;
        system
            .delete_partitions(
                &Session::stateless(identity.user_id, identity.ip_address),
                &query.stream_id.clone(),
                &query.topic_id.clone(),
                query.partitions_count,
            )
            .await?;
    }

    let system = state.system.read().await;
    system
        .state
        .apply(
            identity.user_id,
            EntryCommand::DeletePartitions(DeletePartitions {
                stream_id: query.stream_id.clone(),
                topic_id: query.topic_id.clone(),
                partitions_count: query.partitions_count,
            }),
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
