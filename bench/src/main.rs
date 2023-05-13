mod args;
mod test_args;
mod test_runner;

use crate::args::Args;
use crate::test_args::TestArgs;
use anyhow::Result;
use clap::Parser;
use sdk::client::DisconnectedClient;
use sdk::error::Error;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let test_args = TestArgs {
        messages_per_batch: args.messages_per_batch,
        message_batches: args.message_batches,
        connections_count: args.connections_count,
    };
    tracing_subscriber::fmt::init();

    info!("Starting the benchmarks...");
    let client = DisconnectedClient::new(
        &args.client_address,
        &args.server_address,
        &args.server_name,
    )?;

    let mut client = client.connect().await?;
    test_runner::run_test(&mut client, test_args).await?;
    info!("Finished the benchmarks.");
    Ok(())
}
