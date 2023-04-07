mod args;
mod command;
mod handlers;

use crate::command::Command;
use anyhow::Result;
use bytes::BytesMut;
use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use streaming::system::System;
use tokio::io;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let args = args::Args::parse();
    tracing_subscriber::fmt::init();
    let address = args.address.clone();
    let server = UdpSocket::bind(address).await?;
    let socket_receiver = Arc::new(server);
    let socket_sender = socket_receiver.clone();
    let (sender, mut receiver) = mpsc::channel::<(Vec<u8>, SocketAddr)>(1024);

    let system = System::init().await;
    if let Err(error) = system {
        panic!("Iggy server has finished, due to an error: {}.", error);
    }

    let mut system = system.unwrap();
    info!("Iggy server has started on: {:?}", args.address);

    tokio::spawn(async move {
        while let Some((bytes, address)) = receiver.recv().await {
            Command::try_handle(&bytes, &socket_sender, address, &mut system).await;
        }
    });

    let mut buffer = BytesMut::zeroed(1024);
    loop {
        let (length, address) = socket_receiver.recv_from(&mut buffer).await?;
        info!("{:?} bytes received from {:?}", length, address);
        sender
            .send((buffer[..length].to_vec(), address))
            .await
            .unwrap();
    }
}
