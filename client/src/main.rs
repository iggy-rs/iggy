mod args;
mod client_error;
mod command;
mod handlers;

use crate::args::Args;
use crate::client_error::ClientError;
use anyhow::Result;
use clap::Parser;
use sdk::client::Client;
use std::io;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();

    let client = Client::new(
        &args.client_address,
        &args.server_address,
        &args.server_name,
    )?;
    let mut client = client.connect().await?;
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
