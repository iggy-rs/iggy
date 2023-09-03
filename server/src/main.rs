mod args;
mod binary;
mod components;
mod http;
mod quic;
mod server_command;
mod server_config;
mod server_error;
mod tcp;

use crate::args::Args;
use crate::components::{channel, config_provider, message_cleaner, message_saver};
use crate::http::http_server;
use crate::quic::quic_server;
use crate::server_command::ServerCommand;
use crate::server_config::ServerConfig;
use crate::server_error::ServerError;
use crate::tcp::tcp_server;
use anyhow::Result;
use clap::Parser;
use figlet_rs::FIGfont;
use std::sync::Arc;
use streaming::persister::FileWithSyncPersister;
use streaming::segments::storage::FileSegmentStorage;
use streaming::systems::system::System;
use tokio::signal;
use tokio::sync::RwLock;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), ServerError> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy Server");
    println!("{}", figure.unwrap());
    let config_provider = config_provider::resolve(&args.config_provider)?;
    let config = ServerConfig::load(config_provider.as_ref()).await?;
    let mut system = System::new(config.system.clone(), None);
    system.init().await?;
    let system = Arc::new(RwLock::new(system));
    let (sender, receiver) = flume::unbounded::<ServerCommand>();
    message_cleaner::start(config.message_cleaner, system.clone());
    message_saver::start(config.message_saver, sender.clone());
    channel::start(system.clone(), receiver);

    if config.http.enabled {
        let system = system.clone();
        tokio::spawn(async move {
            http_server::start(config.http, system).await;
        });
    }

    if config.quic.enabled {
        quic_server::start(config.quic, system.clone());
    }

    if config.tcp.enabled {
        tcp_server::start(config.tcp, system.clone());
    }

    match signal::ctrl_c().await {
        Ok(()) => {
            info!("Shutting down Iggy server...");
            let mut system = system.write().await;
            let persister = Arc::new(FileWithSyncPersister);
            let storage = Arc::new(FileSegmentStorage::new(persister));
            system.shutdown(storage).await?;
            info!("Iggy server has shutdown successfully.");
        }
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
        }
    }

    Ok(())
}
