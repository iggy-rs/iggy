mod args;
mod client_error;
mod command;
mod handlers;

use crate::args::Args;
use crate::client_error::ClientError;
use anyhow::Result;
use clap::Parser;
use sdk::quic::client::Client;
use sdk::quic::config::Config;
use std::io;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    let client = Client::create(Config {
        client_address: args.client_address,
        server_address: args.server_address,
        server_name: args.server_name,
        response_buffer_size: args.response_buffer_size,
    })?;
    let client = client.connect().await?;
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

        if let Err(error) = command::handle(&user_input, &client).await {
            error!("Error: {:?}", error);
            continue;
        }
    }
}
