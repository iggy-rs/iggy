mod args;
mod command;
mod components;
mod handlers;
mod sender;
mod server;
mod server_command;
mod server_config;
mod server_error;

use crate::args::Args;
use crate::server::*;
use crate::server_config::ServerConfig;
use crate::server_error::ServerError;
use anyhow::Result;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), ServerError> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();

    let config = ServerConfig::load(&args.config)?;
    let server = ServerSystem::init(config).await?;
    server.start().await?;

    Ok(())
}
