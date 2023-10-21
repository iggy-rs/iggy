pub(crate) mod common;
pub(crate) mod partition;
pub(crate) mod personal_access_token;
pub(crate) mod stream;
pub(crate) mod system;
pub(crate) mod topic;

use crate::args::{
    partition::PartitionAction, personal_access_token::PersonalAccessTokenAction,
    stream::StreamAction, system::PingArgs, topic::TopicAction,
};
use clap::{Parser, Subcommand};
use iggy::args::Args as IggyArgs;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub(crate) struct IggyConsoleArgs {
    #[clap(flatten)]
    pub(crate) iggy: IggyArgs,

    #[clap(subcommand)]
    pub(crate) command: Command,

    /// Quiet mode (disabled stdout printing)
    #[clap(short, long, default_value_t = false)]
    pub(crate) quiet: bool,

    /// Debug mode (verbose printing to given file)
    #[clap(short, long)]
    pub(crate) debug: Option<PathBuf>,

    /// Iggy server username
    #[clap(short, long)]
    pub(crate) username: Option<String>,

    /// Iggy server password
    #[clap(short, long)]
    pub(crate) password: Option<String>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// stream operations
    #[clap(subcommand)]
    Stream(StreamAction),
    /// topic operations
    #[clap(subcommand)]
    Topic(TopicAction),
    /// partition operations
    #[clap(subcommand)]
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
    #[clap(subcommand)]
    Pat(PersonalAccessTokenAction),
}
