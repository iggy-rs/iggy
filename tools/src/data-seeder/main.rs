mod seeder;

use anyhow::Result;
use clap::Parser;
use iggy::args::Args;
use iggy::client::UserClient;
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::users::login_user::LoginUser;
use iggy::utils::crypto::{Aes256GcmEncryptor, Encryptor};
use std::error::Error;
use std::sync::Arc;
use tracing::info;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct DataSeederArgs {
    #[clap(flatten)]
    pub(crate) iggy: Args,

    #[arg(long, default_value = "iggy")]
    pub username: String,

    #[arg(long, default_value = "iggy")]
    pub password: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = DataSeederArgs::parse();
    tracing_subscriber::fmt::init();
    let encryptor: Option<Box<dyn Encryptor>> = match args.iggy.encryption_key.is_empty() {
        true => None,
        false => Some(Box::new(
            Aes256GcmEncryptor::from_base64_key(&args.iggy.encryption_key).unwrap(),
        )),
    };
    info!("Selected transport: {}", args.iggy.transport);
    let username = args.username.clone();
    let password = args.password.clone();
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(args.iggy)?);
    let client = client_provider::get_raw_client(client_provider_config).await?;
    let client = IggyClient::create(client, IggyClientConfig::default(), None, None, encryptor);
    client
        .login_user(&LoginUser { username, password })
        .await
        .unwrap();
    info!("Data seeder has started...");
    seeder::seed(&client).await.unwrap();
    info!("Data seeder has finished.");
    Ok(())
}
