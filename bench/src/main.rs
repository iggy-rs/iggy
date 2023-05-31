mod args;
mod benchmark;
mod benchmark_result;
mod benchmark_runner;
mod client_factory;
mod http;
mod quic;

use crate::args::Args;
use anyhow::Result;
use clap::Parser;
use sdk::error::Error;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    if !args.quic && !args.http {
        panic!("At least one of the protocols must be provided: http or quic.")
    }
    tracing_subscriber::fmt::init();
    info!("Starting the benchmarks...");
    benchmark_runner::run(args).await?;
    info!("Finished the benchmarks.");
    Ok(())
}
