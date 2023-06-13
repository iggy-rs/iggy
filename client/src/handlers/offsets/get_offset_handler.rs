use crate::client_error::ClientError;
use sdk::client::Client;
use shared::offsets::get_offset::GetOffset;
use tracing::info;

pub async fn handle(command: GetOffset, client: &dyn Client) -> Result<(), ClientError> {
    let offset = client.get_offset(command).await?;
    info!("Offset: {:#?}", offset);
    Ok(())
}
