use thiserror::Error;
use tokio::io;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("IO error")]
    IoError(#[from] io::Error),
    #[error("Cannot load configuration")]
    CannotLoadConfiguration,
    #[error("System error")]
    SystemError(#[from] shared::error::Error),
}
