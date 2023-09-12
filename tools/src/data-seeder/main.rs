mod seeder;

use anyhow::Result;
use clap::Parser;
use iggy::args::Args;
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::utils::crypto::{Aes256GcmEncryptor, Encryptor};
use std::error::Error;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    let encryptor: Option<Box<dyn Encryptor>> = match args.encryption_key.is_empty() {
        true => None,
        false => Some(Box::new(
            Aes256GcmEncryptor::from_base64_key(&args.encryption_key).unwrap(),
        )),
    };
    info!("Selected transport: {}", args.transport);
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(args)?);
    let client = client_provider::get_raw_client(client_provider_config).await?;
    let client = IggyClient::create(client, IggyClientConfig::default(), None, None, encryptor);
    info!("Data seeder has started...");
    seeder::seed(&client).await.unwrap();
    info!("Data seeder has finished.");
    Ok(())
}
