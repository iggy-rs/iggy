mod args;
mod cli;
mod cmd;
mod error;
mod logging;

use crate::args::{topic::TopicAction, Command, IggyConsoleArgs, StreamAction};
use crate::cmd::{
    stream::{
        create::StreamCreate, delete::StreamDelete, get::StreamGet, list::StreamList,
        update::StreamUpdate,
    },
    topic::{
        create::TopicCreate, delete::TopicDelete, get::TopicGet, list::TopicList,
        update::TopicUpdate,
    },
};
use crate::error::IggyConsoleError;
use crate::logging::{Logging, PRINT_TARGET};
use clap::Parser;
use cli::CliCommand;
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::utils::crypto::{Aes256GcmEncryptor, Encryptor};
use std::sync::Arc;
use tracing::{event, Level};

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
        Command::Topic(command) => match command {
            TopicAction::Create(args) => Box::new(TopicCreate::new(
                args.stream_id,
                args.topic_id,
                args.partitions_count,
                args.message_expiry,
                args.name.clone(),
            )),
            TopicAction::Delete(args) => Box::new(TopicDelete::new(args.stream_id, args.topic_id)),
            TopicAction::Get(args) => Box::new(TopicGet::new(args.stream_id, args.topic_id)),
            TopicAction::Update(args) => Box::new(TopicUpdate::new(
                args.stream_id,
                args.topic_id,
                args.name.clone(),
                args.message_expiry,
            )),
            TopicAction::List(args) => Box::new(TopicList::new(args.stream_id, args.list_mode)),
        },
    }
}

#[tokio::main]
async fn main() -> Result<(), IggyConsoleError> {
    let args = IggyConsoleArgs::parse();

    let mut logging = Logging::new();
    logging.init(args.quiet, args.debug);

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

    event!(target: PRINT_TARGET, Level::INFO, "Executing {}", command.explain());
    command.execute_cmd(&client).await?;

    Ok(())
}
