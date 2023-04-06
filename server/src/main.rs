mod args;
mod command;
mod handlers;

use anyhow::Result;
use clap::Parser;
use tokio::io;
use tokio::net::UdpSocket;
use crate::command::Command;
use tracing::{info};
use streaming::{system};

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let args = args::Args::parse();
    tracing_subscriber::fmt::init();
    let address = args.address.clone();
    let socket = UdpSocket::bind(address).await?;
    let mut buffer = [0; 1024];
    info!("Iggy server has started on: {:?}", args.address);
    let stream = system::init().await;
    if let Err(error) = stream {
        panic!("Iggy server has finished, due to an error: {}.", error);
    }

    let mut stream = stream.unwrap();
    loop {
        let (length, address) = socket.recv_from(&mut buffer).await?;
        info!("{:?} bytes received from {:?}", length, address);
        Command::try_handle(&buffer[..length], &socket, address, &mut stream).await;
    }
}