use crate::handlers::STATUS_OK;
use anyhow::Result;
use std::net::SocketAddr;
use streaming::error::Error;
use streaming::system::System;
use tokio::net::UdpSocket;

pub const COMMAND: &[u8] = &[10];

pub async fn handle(
    socket: &UdpSocket,
    address: SocketAddr,
    system: &mut System,
) -> Result<(), Error> {
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

    socket
        .send_to([STATUS_OK, streams.as_slice()].concat().as_slice(), address)
        .await?;
    Ok(())
}
