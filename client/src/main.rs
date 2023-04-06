mod command;
mod handlers;
mod message;
mod topic;

use anyhow::Result;
use clap::Parser;
use std::io;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:8081")]
    address: String,

    #[arg(short, long, default_value = "127.0.0.1:8080")]
    server: String,
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    let socket = UdpSocket::bind(args.address.parse::<SocketAddr>().unwrap()).await?;
    let remote_addr = args.server.parse::<SocketAddr>().unwrap();
    let mut user_input = String::new();
    info!(
        "Iggy client has started on: {:?}, connecting to server: {:?}...",
        args.address, args.server
    );
    socket.connect(remote_addr).await?;
    info!("Connected to the server {:?}.", args.server);

    let stdin = io::stdin();
    let mut buffer = [0; 1024];

    loop {
        info!("Enter command to send to the server: ");
        stdin.read_line(&mut user_input)?;
        if user_input.contains('\n') {
            user_input.pop();
        }
        if user_input.contains('\r') {
            user_input.pop();
        }

        socket.writable().await?;
        let response = command::handle(&user_input, &socket, &mut buffer).await;
        user_input.clear();
        if let Err(error) = response {
            error!("Error: {:?}", error);
            continue;
        }
    }
}
