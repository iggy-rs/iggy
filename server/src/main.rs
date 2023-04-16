mod args;
mod command;
mod components;
mod handlers;
mod server;
mod server_command;
mod server_config;

use crate::server::*;
use crate::server_config::ServerConfig;
use anyhow::Result;
use clap::Parser;
use tokio::io;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let args = args::Args::parse();
    tracing_subscriber::fmt::init();

    let config = ServerConfig::load(&args.config);
    let server = ServerSystem::init(config).await?;
    server.start().await?;

    Ok(())
}
