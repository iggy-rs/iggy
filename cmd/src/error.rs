use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum ConsoleError {
    #[error("Iggy client error")]
    IggyClient(#[from] iggy::client_error::ClientError),

    #[error("Iggy sdk or command error")]
    CommandError(#[from] anyhow::Error),

    #[error("Iggy password prompt error")]
    PasswordPrompt(#[from] passterm::PromptError),
}
