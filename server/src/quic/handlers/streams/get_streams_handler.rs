use crate::quic::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::streams::get_streams::GetStreams;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: GetStreams,
    sender: &mut Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    let streams = system
        .get_streams()
        .iter()
        .flat_map(|stream| {
            [
                &stream.id.to_le_bytes(),
                &(stream.get_topics().len() as u32).to_le_bytes(),
                &(stream.name.len() as u32).to_le_bytes(),
                stream.name.as_bytes(),
            ]
            .concat()
        })
        .collect::<Vec<u8>>();

    sender
        .send_ok_response([streams.as_slice()].concat().as_slice())
        .await?;
    Ok(())
}
