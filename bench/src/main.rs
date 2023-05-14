mod args;
mod poll_messages_test;
mod send_messages_test;
mod test_client;
mod test_runner;

use crate::args::Args;
use anyhow::Result;
use clap::Parser;
use sdk::error::Error;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();

    info!("Starting the benchmarks...");
    test_runner::run_tests(args).await?;
    info!("Finished the benchmarks.");
    Ok(())
}
