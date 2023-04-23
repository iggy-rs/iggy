use crate::handlers::STATUS_OK;
use anyhow::Result;
use shared::error::Error;

pub async fn handle(send: &mut quinn::SendStream) -> Result<(), Error> {
    send.write_all(STATUS_OK).await?;
    Ok(())
}
