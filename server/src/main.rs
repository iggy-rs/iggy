mod args;
mod components;
mod http;
mod quic;
mod server_command;
mod server_config;
mod server_error;
mod shared;
mod tcp;

use crate::args::Args;
use crate::components::{channel, message_saver};
use crate::http::http_server;
use crate::quic::quic_server;
use crate::server_command::ServerCommand;
use crate::server_config::ServerConfig;
use crate::server_error::ServerError;
use crate::tcp::tcp_server;
use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use streaming::system::System;
use tokio::signal;
use tokio::sync::RwLock;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), ServerError> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();

    let config = ServerConfig::load(&args.config)?;
    let mut system = System::create(config.system.clone());
    system.init().await?;
    let system = Arc::new(RwLock::new(system));
    let (sender, receiver) = flume::unbounded::<ServerCommand>();
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
            system.shutdown().await?;
            info!("Iggy server has shutdown successfully.");
        }
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
        }
    }

    Ok(())
}
