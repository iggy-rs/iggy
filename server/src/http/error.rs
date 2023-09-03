use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use iggy::error::Error;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CustomError {
    #[error(transparent)]
    Error(#[from] Error),
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub id: u32,
    pub code: String,
    pub reason: String,
    pub field: Option<String>,
}

impl IntoResponse for CustomError {
    fn into_response(self) -> Response {
        match self {
            CustomError::Error(error) => {
                let status_code = match error {
                    Error::StreamIdNotFound(_) => StatusCode::NOT_FOUND,
                    Error::TopicIdNotFound(_, _) => StatusCode::NOT_FOUND,
                    Error::PartitionNotFound(_, _, _) => StatusCode::NOT_FOUND,
                    Error::SegmentNotFound => StatusCode::NOT_FOUND,
                    Error::ClientNotFound(_) => StatusCode::NOT_FOUND,
                    Error::ConsumerGroupNotFound(_, _) => StatusCode::NOT_FOUND,
                    Error::ConsumerGroupMemberNotFound(_, _, _) => StatusCode::NOT_FOUND,
                    Error::IoError(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    Error::WriteError(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    Error::CannotParseInt(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    Error::CannotParseSlice(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    Error::CannotParseUtf8(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    _ => StatusCode::BAD_REQUEST,
                };
                (status_code, Json(ErrorResponse::from_error(error)))
            }
        }
        .into_response()
    }
}

impl ErrorResponse {
    pub fn from_error(error: Error) -> Self {
        ErrorResponse {
            id: error.as_code(),
            code: error.as_string().to_string(),
            reason: error.to_string(),
            field: match error {
                Error::StreamIdNotFound(_) => Some("stream_id".to_string()),
                Error::TopicIdNotFound(_, _) => Some("topic_id".to_string()),
                Error::PartitionNotFound(_, _, _) => Some("partition_id".to_string()),
                Error::SegmentNotFound => Some("segment_id".to_string()),
                Error::ClientNotFound(_) => Some("client_id".to_string()),
                Error::InvalidStreamName => Some("name".to_string()),
                Error::StreamNameAlreadyExists(_) => Some("name".to_string()),
                Error::InvalidTopicName => Some("name".to_string()),
                Error::TopicNameAlreadyExists(_, _) => Some("name".to_string()),
                Error::InvalidStreamId => Some("stream_id".to_string()),
                Error::StreamIdAlreadyExists(_) => Some("stream_id".to_string()),
                Error::InvalidTopicId => Some("topic_id".to_string()),
                Error::TopicIdAlreadyExists(_, _) => Some("topic_id".to_string()),
                Error::InvalidOffset(_) => Some("offset".to_string()),
                Error::InvalidConsumerGroupId => Some("consumer_group_id".to_string()),
                Error::ConsumerGroupAlreadyExists(_, _) => Some("consumer_group_id".to_string()),
                _ => None,
            },
        }
    }
}
