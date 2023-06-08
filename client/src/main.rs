mod args;
mod client_error;
mod client_provider;
mod command;
mod handlers;

use crate::args::Args;
use crate::client_error::ClientError;
use anyhow::Result;
use clap::Parser;
use sdk::error::Error;
use std::io;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    info!("Selected transport: {}", args.transport);
    let mut client = client_provider::get_client(args).await?;
    let client = client.as_mut();
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

        if let Err(error) = command::handle(&user_input, client).await {
            error!("Error: {:?}", error);
            match error {
                ClientError::SdkError(Error::NotConnected) => {
                    error!("Client is not connected. Client will be reconnected.");
                    client.connect().await?;
                }
                ClientError::SdkError(Error::ConnectionError(error)) => {
                    error!("Connection error: {}. Client will be reconnected.", error);
                    client.connect().await?;
                }
                ClientError::SdkError(Error::IoError(error)) => {
                    error!("Connection error: {}. Client will be reconnected.", error);
                    client.connect().await?;
                }
                _ => {}
            }

            continue;
        }
    }
}
