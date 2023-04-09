mod args;
mod command;
mod handlers;
mod server;
mod server_command;

use anyhow::Result;
use clap::Parser;
use tokio::io;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let args = args::Args::parse();
    tracing_subscriber::fmt::init();

    let server = server::init(args.address).await?;
    server::start_watcher(server.sender.clone());
    server::start_channel(server.system, server.receiver, server.socket.clone());
    server::start_listener(server.socket, server.sender).await?;

    Ok(())
}
