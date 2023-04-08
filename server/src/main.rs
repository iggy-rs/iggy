mod args;
mod command;
mod handlers;
mod server_command;

use crate::command::Command;
use crate::server_command::ServerCommand;
use anyhow::Result;
use bytes::BytesMut;
use clap::Parser;
use std::sync::Arc;
use std::time::Duration;
use streaming::system::System;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::{io, task, time};
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let args = args::Args::parse();
    tracing_subscriber::fmt::init();
    let address = args.address.clone();
    let server = UdpSocket::bind(address).await?;
    let socket_receiver = Arc::new(server);
    let socket_sender = socket_receiver.clone();
    let (sender, mut receiver) = mpsc::channel::<ServerCommand>(1024);
    let interval_sender = sender.clone();

    let system = System::init().await;
    if let Err(error) = system {
        panic!("Iggy server has finished, due to an error: {}.", error);
    }

    let mut system = system.unwrap();
    info!("Iggy server has started on: {:?}", args.address);

    tokio::spawn(async move {
        while let Some(server_command) = receiver.recv().await {
            match server_command {
                ServerCommand::HandleRequest(bytes, address) => {
                    Command::try_handle(&bytes, &socket_sender, address, &mut system).await;
                }
                ServerCommand::SaveMessages => {
                    if system.save_existing_messages().await.is_err() {
                        error!("Couldn't save existing messages on disk.");
                    }
                }
            }
        }
    });

    // TODO: Make this configurable.
    task::spawn(async move {
        let duration = Duration::from_secs(10);
        let mut interval = time::interval(duration);
        info!(
            "Existing messages will be automatically saved every: {:?}.",
            duration
        );
        interval.tick().await;
        loop {
            interval.tick().await;
            interval_sender
                .send(ServerCommand::SaveMessages)
                .await
                .unwrap();
        }
    });

    loop {
        let mut buffer = BytesMut::zeroed(1024);
        let (length, address) = socket_receiver.recv_from(&mut buffer).await?;
        buffer.truncate(length);
        info!("{:?} bytes received from {:?}", length, address);
        sender
            .send(ServerCommand::HandleRequest(buffer.freeze(), address))
            .await
            .unwrap();
    }
}
