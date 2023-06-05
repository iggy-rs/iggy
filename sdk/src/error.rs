use quinn::{ConnectionError, ReadError, ReadToEndError, WriteError};
use std::array::TryFromSliceError;
use std::net::AddrParseError;
use std::str::Utf8Error;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Empty response")]
    EmptyResponse,
    #[error("Invalid configuration")]
    InvalidConfiguration,
    #[error("Not connected")]
    NotConnected,
    #[error("Request error")]
    RequestError(#[from] reqwest::Error),
    #[error("Request middleware error")]
    RequestMiddlewareError(#[from] reqwest_middleware::Error),
    #[error("Cannot create endpoint")]
    CannotCreateEndpoint,
    #[error("Cannot parse URL")]
    CannotParseUrl,
    #[error("Invalid response: {0}")]
    InvalidResponse(u8),
    #[error("Cannot parse integer")]
    CannotParseSlice(#[from] TryFromSliceError),
    #[error("Cannot parse UTF8")]
    CannotParseUtf8(#[from] Utf8Error),
    #[error("Cannot parse address")]
    CannotParseAddress(#[from] AddrParseError),
    #[error("Write error")]
    WriteError(#[from] WriteError),
    #[error("Read error")]
    ReadError(#[from] ReadError),
    #[error("Connection error")]
    ConnectionError(#[from] ConnectionError),
    #[error("Read to end error")]
    ReadToEndError(#[from] ReadToEndError),
}
