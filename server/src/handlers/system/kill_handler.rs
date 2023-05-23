use crate::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::system::kill::Kill;
use tracing::trace;

pub async fn handle(command: Kill, sender: &mut Sender) -> Result<(), Error> {
    trace!("{}", command);
    #[cfg(feature = "allow_kill_command")]
    {
        sender.send_empty_ok_response().await?;
        std::process::exit(0);
    }
    #[cfg(not(feature = "allow_kill_command"))]
    sender.send_error_response(Error::InvalidCommand).await
}
