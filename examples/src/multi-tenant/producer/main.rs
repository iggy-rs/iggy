use clap::Parser;
use iggy_examples::shared::args::Args;
use std::error::Error;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    info!(
        "Multi-tenant producer has started, selected transport: {}",
        args.transport
    );
    Ok(())
}
