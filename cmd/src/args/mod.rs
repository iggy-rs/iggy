pub(crate) mod client;
pub(crate) mod common;
pub(crate) mod partition;
pub(crate) mod permissions;
pub(crate) mod personal_access_token;
pub(crate) mod stream;
pub(crate) mod system;
pub(crate) mod topic;
pub(crate) mod user;

use self::user::UserAction;
use crate::args::{
    client::ClientAction, partition::PartitionAction,
    personal_access_token::PersonalAccessTokenAction, stream::StreamAction, system::PingArgs,
    topic::TopicAction,
};
use clap::{Args, Command as ClapCommand};
use clap::{Parser, Subcommand};
use clap_complete::{generate, Generator, Shell};
use figlet_rs::FIGfont;
use iggy::args::Args as IggyArgs;
use std::path::PathBuf;

const QUIC_TRANSPORT: &str = "quic";
const HTTP_TRANSPORT: &str = "http";
const TCP_TRANSPORT: &str = "tcp";

static CARGO_BIN_NAME: &str = env!("CARGO_BIN_NAME");
static CARGO_PKG_HOMEPAGE: &str = env!("CARGO_PKG_HOMEPAGE");

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub(crate) struct IggyConsoleArgs {
    #[clap(flatten, verbatim_doc_comment)]
    pub(crate) iggy: IggyArgs,

    #[clap(subcommand)]
    pub(crate) command: Option<Command>,

    /// Quiet mode (disabled stdout printing)
    #[clap(short, long, default_value_t = false)]
    pub(crate) quiet: bool,

    /// Debug mode (verbose printing to given file)
    #[clap(short, long)]
    pub(crate) debug: Option<PathBuf>,

    /// Iggy server username
    #[clap(short, long, group = "credentials")]
    pub(crate) username: Option<String>,

    /// Iggy server password
    ///
    /// An optional parameter to specify the password for authentication.
    /// If not provided, user will be prompted interactively to enter the
    /// password securely.
    #[clap(short, long, verbatim_doc_comment)]
    pub(crate) password: Option<String>,

    /// Iggy server personal access token
    #[clap(short, long, group = "credentials")]
    pub(crate) token: Option<String>,

    /// Iggy server personal access token name
    ///
    /// When personal access token is created using command line tool and stored
    /// inside platform-specific secure storage its name can be used as a value
    /// for this option without revealing the token value.
    #[clap(short = 'n', long, group = "credentials", verbatim_doc_comment)]
    pub(crate) token_name: Option<String>,

    /// Shell completion generator for iggy command
    ///
    /// Option prints shell completion code on standard output for selected shell.
    /// Redirect standard output to file and follow and use selected shell means
    /// to enable completion for iggy command.
    /// Option cannot be combined with other options.
    ///
    /// Example:
    ///  source <(iggy --generate bash)
    /// or
    ///  iggy --generate bash > iggy_completion.bash
    ///  source iggy_completion.bash
    #[clap(verbatim_doc_comment)]
    #[clap(long = "generate", value_enum)]
    pub(crate) generator: Option<Shell>,
}

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum Command {
    /// stream operations
    #[command(subcommand, visible_alias = "s")]
    Stream(StreamAction),
    /// topic operations
    #[command(subcommand, visible_alias = "t")]
    Topic(TopicAction),
    /// partition operations
    #[command(subcommand, visible_alias = "p")]
    Partition(PartitionAction),
    /// ping iggy server
    ///
    /// Check if iggy server is up and running and what's the response ping response time
    Ping(PingArgs),
    /// get current client info
    ///
    /// Command connects to Iggy server and collects client info like client ID, user ID
    /// server address and protocol type.
    Me,
    /// get iggy server statistics
    ///
    /// Collect basic Iggy server statistics like number of streams, topics, partitions, etc.
    /// Server OS name, version, etc. are also collected.
    Stats,
    /// personal access token operations
    #[command(subcommand)]
    Pat(PersonalAccessTokenAction),
    /// user operations
    #[command(subcommand, visible_alias = "u")]
    User(UserAction),
    /// client operations
    #[command(subcommand, visible_alias = "c")]
    Client(ClientAction),
}

impl IggyConsoleArgs {
    pub(crate) fn get_server_address(&self) -> Option<String> {
        match self.iggy.transport.as_str() {
            QUIC_TRANSPORT => Some(
                self.iggy
                    .quic_server_address
                    .split(':')
                    .next()
                    .unwrap()
                    .into(),
            ),
            HTTP_TRANSPORT => Some(
                self.iggy
                    .http_api_url
                    .clone()
                    .replace("http://", "")
                    .replace("localhost", "127.0.0.1")
                    .split(':')
                    .next()
                    .unwrap()
                    .into(),
            ),
            TCP_TRANSPORT => Some(
                self.iggy
                    .tcp_server_address
                    .split(':')
                    .next()
                    .unwrap()
                    .into(),
            ),
            _ => None,
        }
    }

    pub(crate) fn generate_completion<G: Generator>(&self, generator: G) {
        generate(
            generator,
            &mut IggyConsoleArgs::augment_args_for_update(
                ClapCommand::new(CARGO_BIN_NAME).bin_name(CARGO_BIN_NAME),
            ),
            CARGO_BIN_NAME,
            &mut std::io::stdout(),
        );
    }

    pub(crate) fn print_overview() {
        let mut cli = IggyConsoleArgs::augment_args_for_update(
            ClapCommand::new(CARGO_BIN_NAME).bin_name(CARGO_BIN_NAME),
        );

        let full_help = cli.render_help().to_string();
        let help = full_help.replace(
            &full_help[full_help.find("Options:").unwrap()..full_help.len()],
            "",
        );

        let standard_font = FIGfont::standard().unwrap();
        let figure = standard_font.convert("Iggy CLI").unwrap();

        println!("{figure}");
        println!("{help}");
        println!("Run '{CARGO_BIN_NAME} --help' for full help message.");
        println!("Run '{CARGO_BIN_NAME} COMMAND --help' for more information on a command.");
        println!();
        println!("For more help on what's Iggy and how to use it, head to {CARGO_PKG_HOMEPAGE}");
    }
}
