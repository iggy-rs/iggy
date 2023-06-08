mod args;
mod benchmark;
mod benchmark_result;
mod benchmark_runner;
mod benchmarks;
mod client_factory;
mod http;
mod initializer;
mod quic;
mod tcp;

use crate::args::Args;
use anyhow::Result;
use clap::Parser;
use sdk::error::Error;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    args.validate()?;
    info!("Starting the benchmarks...");
    benchmark_runner::run(args).await?;
    info!("Finished the benchmarks.");
    Ok(())
}
