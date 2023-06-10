use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use shared::error::Error;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CustomError {
    #[error(transparent)]
    Error(#[from] Error),
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub id: u8,
    pub code: String,
    pub reason: String,
}

impl IntoResponse for CustomError {
    fn into_response(self) -> Response {
        match self {
            CustomError::Error(error) => {
                let status_code = match error {
                    Error::StreamNotFound(_) => StatusCode::NOT_FOUND,
                    Error::TopicNotFound(_, _) => StatusCode::NOT_FOUND,
                    Error::PartitionNotFound(_) => StatusCode::NOT_FOUND,
                    Error::SegmentNotFound => StatusCode::NOT_FOUND,
                    Error::LogFileNotFound => StatusCode::NOT_FOUND,
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
            code: error.as_text_code().to_string(),
            reason: error.to_string(),
        }
    }
}
