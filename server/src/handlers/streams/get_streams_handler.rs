use crate::handlers::STATUS_OK;
use anyhow::Result;
use shared::error::Error;
use streaming::system::System;

pub async fn handle(send: &mut quinn::SendStream, system: &mut System) -> Result<(), Error> {
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

    send.write_all([STATUS_OK, streams.as_slice()].concat().as_slice())
        .await?;
    Ok(())
}
