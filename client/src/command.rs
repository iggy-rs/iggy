use crate::handlers;
use std::io;
use tokio::net::UdpSocket;
use tracing::info;

pub async fn handle(input: &str, socket: &UdpSocket, buffer: &mut [u8; 1024]) -> io::Result<()> {
    let parts = input.split('|').collect::<Vec<&str>>();
    let command = parts[0];
    info!("Handling '{:#}' command...", command);
    match command {
        "ping" => handlers::ping_handler::handle(socket, buffer).await,
        "get_topics" => handlers::get_topics_handler::handle(socket, buffer).await,
        _ => {
            if parts.len() < 2 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Invalid command parts.",
                ));
            }

            let input = &parts[1..];
            match command {
                "create_topic" => {
                    handlers::create_topic_handler::handle(input, socket, buffer).await
                }
                "delete_topic" => {
                    handlers::delete_topic_handler::handle(input, socket, buffer).await
                }
                "poll" => handlers::poll_handler::handle(input, socket, buffer).await,
                "send" => handlers::send_handler::handle(input, socket, buffer).await,
                _ => Err(io::Error::new(io::ErrorKind::Other, "Invalid command.")),
            }
        }
    }
}
