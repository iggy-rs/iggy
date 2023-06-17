use anyhow::Result;
use clap::Parser;
use sdk::args::Args;
use std::error::Error;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    info!(
        "Consumer has started, selected transport: {}",
        args.transport
    );
    // TODO: Implement sample consumer
    Ok(())
}
