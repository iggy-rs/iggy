mod client_error;
mod command;
mod handlers;
mod test_mode;

use crate::client_error::ClientError;
use anyhow::Result;
use clap::Parser;
use sdk::client::DisconnectedClient;
use std::io;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:0")]
    client_address: String,

    #[arg(long, default_value = "127.0.0.1:8080")]
    server_address: String,

    #[arg(long, default_value = "localhost")]
    server_name: String,

    #[arg(short, long)]
    test: bool,
}

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();

    let client = DisconnectedClient::new(
        &args.client_address,
        &args.server_address,
        &args.server_name,
    )?;
    let mut client = client.connect().await?;

    if args.test {
        test_mode::run_test(&mut client).await?;
        return Ok(());
    }

    let stdin = io::stdin();
    let mut user_input = String::new();

    loop {
        info!("Enter command to send to the server: ");
        user_input.clear();
        stdin.read_line(&mut user_input)?;
        if user_input.contains('\n') {
            user_input.pop();
        }
        if user_input.contains('\r') {
            user_input.pop();
        }

        if let Err(error) = command::handle(&user_input, &mut client).await {
            error!("Error: {:?}", error);
            continue;
        }
    }
}
