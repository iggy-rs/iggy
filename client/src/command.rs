use crate::handlers::messages::*;
use crate::handlers::streams::*;
use crate::handlers::system::*;
use crate::handlers::topics::*;
use std::io;
use tokio::net::UdpSocket;
use tracing::info;

pub async fn handle(input: &str, socket: &UdpSocket, buffer: &mut [u8; 1024]) -> io::Result<()> {
    let parts = input.split('|').collect::<Vec<&str>>();
    let command = parts[0];
    info!("Handling '{:#}' command...", command);
    match command {
        "ping" => ping_handler::handle(socket, buffer).await,
        "stream.list" => get_streams_handler::handle(socket, buffer).await,
        _ => {
            if parts.len() < 2 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Invalid command parts.",
                ));
            }

            let input = &parts[1..];
            match command {
                "stream.create" => create_stream_handler::handle(input, socket, buffer).await,
                "stream.delete" => delete_stream_handler::handle(input, socket, buffer).await,
                "topic.list" => get_topics_handler::handle(input, socket, buffer).await,
                "topic.create" => create_topic_handler::handle(input, socket, buffer).await,
                "topic.delete" => delete_topic_handler::handle(input, socket, buffer).await,
                "message.poll" => poll_handler::handle(input, socket, buffer).await,
                "message.send" => send_handler::handle(input, socket, buffer).await,
                _ => Err(io::Error::new(io::ErrorKind::Other, "Invalid command.")),
            }
        }
    }
}
