mod command;
mod consumer_groups;
mod messages;
mod offsets;
mod partitions;
mod streams;
mod system;
mod topics;

use anyhow::Result;
use clap::Parser;
use iggy::args::Args;
use iggy::client_error::ClientError;
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::error::Error;
use std::io;
use std::sync::Arc;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    info!("Selected transport: {}", args.transport);
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(args)?);
    let mut client = client_provider::get_client(client_provider_config).await?;
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
