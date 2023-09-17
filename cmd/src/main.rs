mod args;
mod cli;
mod error;
mod stream;

use crate::args::{IggyConsoleArgs, StreamAction};
use crate::error::IggyConsoleError;
use crate::stream::{
    create::StreamCreate, delete::StreamDelete, get::StreamGet, list::StreamList,
    update::StreamUpdate,
};
use args::Command;
use clap::Parser;
use cli::CliCommand;
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::utils::crypto::{Aes256GcmEncryptor, Encryptor};
use std::sync::Arc;

fn get_command(command: &Command) -> Box<dyn CliCommand> {
    #[warn(clippy::let_and_return)]
    match command {
        Command::Stream(command) => match command {
            StreamAction::List(args) => Box::new(StreamList::new(args.list_mode)),
            StreamAction::Create { id, name } => Box::new(StreamCreate::new(*id, name.clone())),
            StreamAction::Delete { id } => Box::new(StreamDelete::new(*id)),
            StreamAction::Get { id } => Box::new(StreamGet::new(*id)),
            StreamAction::Update { id, name } => Box::new(StreamUpdate::new(*id, name.clone())),
        },
    }
}

#[tokio::main]
async fn main() -> Result<(), IggyConsoleError> {
    let args = IggyConsoleArgs::parse();

    let encryptor: Option<Box<dyn Encryptor>> = match args.iggy.encryption_key.is_empty() {
        true => None,
        false => Some(Box::new(
            Aes256GcmEncryptor::from_base64_key(&args.iggy.encryption_key).unwrap(),
        )),
    };
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(args.iggy.clone())?);
    let client = client_provider::get_raw_client(client_provider_config).await?;
    let client = IggyClient::create(client, IggyClientConfig::default(), None, None, encryptor);

    let mut command = get_command(&args.command);

    println!("Executing {}", command.explain());
    command.execute_cmd(&client).await?;

    Ok(())
}
