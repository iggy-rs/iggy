use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum ConsoleError {
    #[error("Iggy client error")]
    IggyClientError(#[from] iggy::client_error::ClientError),

    #[error("Iggy sdk or command error")]
    IggyCommandError(#[from] anyhow::Error),

    #[error("Iggy password prompt error")]
    IggyPasswordPromptError(#[from] passterm::PromptError),
}
