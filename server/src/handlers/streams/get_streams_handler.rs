use crate::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use streaming::system::System;

pub async fn handle(sender: &mut Sender, system: &mut System) -> Result<(), Error> {
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
