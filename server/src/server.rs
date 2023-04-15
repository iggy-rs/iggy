use crate::command::Command;
use crate::server_command::ServerCommand;
use crate::server_config::ServerConfig;
use anyhow::Result;
use bytes::BytesMut;
use std::sync::Arc;
use std::time::Duration;
use std::{io, process};
use streaming::system::System;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::{task, time};
use tracing::{error, info};

const NAME: &str = "Iggy";

pub struct Server {
    pub system: System,
    pub socket: Arc<UdpSocket>,
    pub sender: mpsc::Sender<ServerCommand>,
    pub receiver: mpsc::Receiver<ServerCommand>,
}

pub async fn init(config: ServerConfig) -> Result<Server, io::Error> {
    info!("Initializing {} server...", NAME);
    let socket = UdpSocket::bind(config.address.clone()).await?;
    let socket = Arc::new(socket);
    let (sender, receiver) = mpsc::channel::<ServerCommand>(1024);

    let system = System::init(config.system).await;
    if let Err(error) = system {
        panic!("{} server has finished, due to an error: {}.", NAME, error);
    }

    let system = system.unwrap();
    let server = Server {
        system,
        socket,
        sender,
        receiver,
    };

    info!("{} server has started on: {:?}", NAME, config.address);
    Ok(server)
}

pub fn handle_shutdown(sender: mpsc::Sender<ServerCommand>) {
    ctrlc::set_handler(move || {
        info!("Shutting down {} server...", NAME);
        let runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
        let shutdown = sender.send(ServerCommand::Shutdown);
        runtime.block_on(shutdown).unwrap();
    })
    .expect("Error setting Ctrl-C handler");
}

// TODO: Make this configurable.
pub fn start_watcher(sender: mpsc::Sender<ServerCommand>) {
    task::spawn(async move {
        let duration = Duration::from_secs(30);
        let mut interval = time::interval(duration);
        info!(
            "Buffered messages will be automatically saved every: {:?}.",
            duration
        );
        interval.tick().await;
        loop {
            interval.tick().await;
            sender.send(ServerCommand::SaveMessages).await.unwrap();
        }
    });
}

pub fn start_channel(
    mut system: System,
    mut receiver: mpsc::Receiver<ServerCommand>,
    socket: Arc<UdpSocket>,
) {
    tokio::spawn(async move {
        while let Some(server_command) = receiver.recv().await {
            match server_command {
                ServerCommand::HandleRequest(bytes, address) => {
                    Command::try_handle(&bytes, &socket, address, &mut system).await;
                }
                ServerCommand::SaveMessages => {
                    if system.persist_messages().await.is_err() {
                        error!("Couldn't save buffered messages on disk.");
                    }
                }
                ServerCommand::Shutdown => {
                    if system.persist_messages().await.is_err() {
                        error!("Couldn't save buffered messages on disk.");
                        process::exit(1);
                    }
                    info!("{} server has shutdown successfully.", NAME);
                    process::exit(0);
                }
            }
        }
    });
}

pub async fn start_listener(
    socket: Arc<UdpSocket>,
    sender: mpsc::Sender<ServerCommand>,
) -> Result<(), io::Error> {
    loop {
        let mut buffer = BytesMut::zeroed(1024);
        let (length, address) = socket.recv_from(&mut buffer).await?;
        buffer.truncate(length);
        info!("{:?} bytes received from {:?}", length, address);
        sender
            .send(ServerCommand::HandleRequest(buffer.freeze(), address))
            .await
            .unwrap();
    }
}
