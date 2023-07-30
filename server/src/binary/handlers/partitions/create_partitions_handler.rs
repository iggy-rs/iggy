use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::error::Error;
use iggy::partitions::create_partitions::CreatePartitions;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &CreatePartitions,
    _sender: &mut dyn Sender,
    _system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    // TODO: Implement create partitions.
    Ok(())
}
