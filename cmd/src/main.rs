mod args;
mod credentials;
mod error;
mod logging;

use crate::args::{
    personal_access_token::PersonalAccessTokenAction, stream::StreamAction, topic::TopicAction,
    Command, IggyConsoleArgs,
};
use crate::credentials::IggyCredentials;
use crate::error::IggyCmdError;
use crate::logging::Logging;
use args::partition::PartitionAction;
use clap::Parser;
use iggy::cli_command::{CliCommand, PRINT_TARGET};
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::cmd::{
    partitions::{create_partitions::CreatePartitionsCmd, delete_partitions::DeletePartitionsCmd},
    personal_access_tokens::{
        create_personal_access_token::CreatePersonalAccessTokenCmd,
        delete_personal_access_tokens::DeletePersonalAccessTokenCmd,
        get_personal_access_tokens::GetPersonalAccessTokensCmd,
    },
    streams::{
        create_stream::CreateStreamCmd, delete_stream::DeleteStreamCmd, get_stream::GetStreamCmd,
        get_streams::GetStreamsCmd, update_stream::UpdateStreamCmd,
    },
    system::{me::GetMeCmd, ping::PingCmd, stats::GetStatsCmd},
    topics::{
        create_topic::CreateTopicCmd, delete_topic::DeleteTopicCmd, get_topic::GetTopicCmd,
        get_topics::GetTopicsCmd, update_topic::UpdateTopicCmd,
    },
    utils::{
        message_expiry::MessageExpiry, personal_access_token_expiry::PersonalAccessTokenExpiry,
    },
};
use iggy::utils::crypto::{Aes256GcmEncryptor, Encryptor};
use std::sync::Arc;
use tracing::{event, Level};

fn get_command(args: &IggyConsoleArgs) -> Box<dyn CliCommand> {
    #[warn(clippy::let_and_return)]
    match &args.command {
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
            PartitionAction::Create(args) => Box::new(CreatePartitionsCmd::new(
                args.stream_id.clone(),
                args.topic_id.clone(),
                args.partitions_count,
            )),
            PartitionAction::Delete(args) => Box::new(DeletePartitionsCmd::new(
                args.stream_id.clone(),
                args.topic_id.clone(),
                args.partitions_count,
            )),
        },
        Command::Ping(args) => Box::new(PingCmd::new(args.count)),
        Command::Me => Box::new(GetMeCmd::new()),
        Command::Stats => Box::new(GetStatsCmd::new()),
        Command::Pat(command) => match command {
            PersonalAccessTokenAction::Create(pat_create_args) => {
                Box::new(CreatePersonalAccessTokenCmd::new(
                    pat_create_args.name.clone(),
                    PersonalAccessTokenExpiry::new(pat_create_args.expiry.clone()),
                    args.quiet,
                    pat_create_args.store_token,
                    args.get_server_address().unwrap(),
                ))
            }
            PersonalAccessTokenAction::Delete(pat_delete_args) => {
                Box::new(DeletePersonalAccessTokenCmd::new(
                    pat_delete_args.name.clone(),
                    args.get_server_address().unwrap(),
                ))
            }
            PersonalAccessTokenAction::List(pat_list_args) => Box::new(
                GetPersonalAccessTokensCmd::new(pat_list_args.list_mode.into()),
            ),
        },
    }
}

#[tokio::main]
async fn main() -> Result<(), IggyCmdError> {
    let args = IggyConsoleArgs::parse();

    let mut logging = Logging::new();
    logging.init(args.quiet, &args.debug);

    // Get command based on command line arguments
    let mut command = get_command(&args);

    // Create credentials based on command line arguments and command
    let mut credentials = IggyCredentials::new(&args, command.login_required())?;

    let encryptor: Option<Box<dyn Encryptor>> = match args.iggy.encryption_key.is_empty() {
        true => None,
        false => Some(Box::new(
            Aes256GcmEncryptor::from_base64_key(&args.iggy.encryption_key).unwrap(),
        )),
    };
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(args.iggy.clone())?);

    let client = client_provider::get_raw_client(client_provider_config).await?;
    let client = IggyClient::create(client, IggyClientConfig::default(), None, None, encryptor);

    credentials.set_iggy_client(&client);
    credentials.login_user().await?;

    event!(target: PRINT_TARGET, Level::INFO, "Executing {}", command.explain());
    command.execute_cmd(&client).await?;

    credentials.logout_user().await?;

    Ok(())
}
