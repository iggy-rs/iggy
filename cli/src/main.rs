mod command;
mod consumer_groups;
mod consumer_offsets;
mod messages;
mod partitions;
mod streams;
mod system;
mod topics;
mod users;

use anyhow::Result;
use clap::Parser;
use figlet_rs::FIGfont;
use iggy::args::Args;
use iggy::client::Client;
use iggy::client_error::ClientError;
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::error::Error;
use iggy::utils::crypto::{Aes256GcmEncryptor, Encryptor};
use std::io;
use std::sync::Arc;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy CLI");
    println!("{}", figure.unwrap());
    let encryptor: Option<Box<dyn Encryptor>> = match args.encryption_key.is_empty() {
        true => None,
        false => Some(Box::new(
            Aes256GcmEncryptor::new_from_base64_key(&args.encryption_key).unwrap(),
        )),
    };
    info!("Selected transport: {}", args.transport);
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(args)?);
    let client = client_provider::get_client(client_provider_config).await?;
    let mut client = IggyClient::new(client, IggyClientConfig::default(), None, encryptor);
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
            match error {
                ClientError::InvalidCommand => {
                    error!("Invalid command: {}", user_input);
                }
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
