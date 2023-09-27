mod args;
mod cmd;
mod error;
mod logging;
mod login;

use crate::args::{stream::StreamAction, topic::TopicAction, Command, IggyConsoleArgs};
use crate::cmd::partition::{create::PartitionCreate, delete::PartitionDelete};
use crate::error::ConsoleError;
use crate::logging::Logging;
use crate::login::{get_password, login_user, logout_user};
use args::partition::PartitionAction;
use clap::Parser;
use iggy::cli_command::{CliCommand, PRINT_TARGET};
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::utils::crypto::{Aes256GcmEncryptor, Encryptor};
use iggy::utils::message_expire::MessageExpiry;
use iggy::{
    streams::{
        create_stream::CreateStreamCmd, delete_stream::DeleteStreamCmd, get_stream::GetStreamCmd,
        get_streams::GetStreamsCmd, update_stream::UpdateStreamCmd,
    },
    topics::{
        create_topic::CreateTopicCmd, delete_topic::DeleteTopicCmd, get_topic::GetTopicCmd,
        get_topics::GetTopicsCmd, update_topic::UpdateTopicCmd,
    },
};
use std::sync::Arc;
use tracing::{event, Level};

fn get_command(command: &Command) -> Box<dyn CliCommand> {
    #[warn(clippy::let_and_return)]
    match command {
        Command::Stream(command) => match command {
            StreamAction::Create(args) => {
                Box::new(CreateStreamCmd::new(args.stream_id, args.name.clone()))
            }
            StreamAction::Delete(args) => Box::new(DeleteStreamCmd::new(args.stream_id.clone())),
            StreamAction::Update(args) => Box::new(UpdateStreamCmd::new(
                args.stream_id.clone(),
                args.name.clone(),
            )),
            StreamAction::Get(args) => Box::new(GetStreamCmd::new(args.stream_id.clone())),
            StreamAction::List(args) => Box::new(GetStreamsCmd::new(args.list_mode.into())),
        },
        Command::Topic(command) => match command {
            TopicAction::Create(args) => Box::new(CreateTopicCmd::new(
                args.stream_id.clone(),
                args.topic_id,
                args.partitions_count,
                args.name.clone(),
                MessageExpiry::new(args.message_expiry.clone()),
            )),
            TopicAction::Delete(args) => Box::new(DeleteTopicCmd::new(
                args.stream_id.clone(),
                args.topic_id.clone(),
            )),
            TopicAction::Update(args) => Box::new(UpdateTopicCmd::new(
                args.stream_id.clone(),
                args.topic_id.clone(),
                args.name.clone(),
                MessageExpiry::new(args.message_expiry.clone()),
            )),
            TopicAction::Get(args) => Box::new(GetTopicCmd::new(
                args.stream_id.clone(),
                args.topic_id.clone(),
            )),
            TopicAction::List(args) => Box::new(GetTopicsCmd::new(
                args.stream_id.clone(),
                args.list_mode.into(),
            )),
        },
        Command::Partition(command) => match command {
            PartitionAction::Create(args) => Box::new(PartitionCreate::new(
                args.stream_id.clone(),
                args.topic_id.clone(),
                args.partitions_count,
            )),
            PartitionAction::Delete(args) => Box::new(PartitionDelete::new(
                args.stream_id.clone(),
                args.topic_id.clone(),
                args.partitions_count,
            )),
        },
    }
}

#[tokio::main]
async fn main() -> Result<(), ConsoleError> {
    let args = IggyConsoleArgs::parse();

    let mut logging = Logging::new();
    logging.init(args.quiet, args.debug);

    let password = get_password(args.password)?;

    let encryptor: Option<Box<dyn Encryptor>> = match args.iggy.encryption_key.is_empty() {
        true => None,
        false => Some(Box::new(
            Aes256GcmEncryptor::from_base64_key(&args.iggy.encryption_key).unwrap(),
        )),
    };
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(args.iggy.clone())?);
    let client = client_provider::get_raw_client(client_provider_config).await?;
    let client = IggyClient::create(client, IggyClientConfig::default(), None, None, encryptor);

    login_user(&client, args.username, password).await?;

    let mut command = get_command(&args.command);

    event!(target: PRINT_TARGET, Level::INFO, "Executing {}", command.explain());
    command.execute_cmd(&client).await?;

    logout_user(&client).await?;

    Ok(())
}
