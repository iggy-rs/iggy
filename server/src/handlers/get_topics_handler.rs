use crate::handlers::STATUS_OK;
use anyhow::Result;
use std::net::SocketAddr;
use streaming::stream::Stream;
use streaming::stream_error::StreamError;
use tokio::net::UdpSocket;
use tracing::info;

pub const COMMAND: &[u8] = &[10];

pub async fn handle(
    socket: &UdpSocket,
    address: SocketAddr,
    stream: &mut Stream,
) -> Result<(), StreamError> {
    info!("Get topics...");
    let topics = stream
        .topics
        .values()
        .flat_map(|topic| {
            [
                &topic.id.to_le_bytes(),
                &(topic.partitions.len() as u32).to_le_bytes(),
                &(topic.name.len() as u32).to_le_bytes(),
                topic.name.as_bytes(),
            ]
            .concat()
        })
        .collect::<Vec<u8>>();

    socket
        .send_to([STATUS_OK, topics.as_slice()].concat().as_slice(), address)
        .await?;
    Ok(())
}
