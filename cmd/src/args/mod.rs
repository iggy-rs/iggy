pub(crate) mod common;
pub(crate) mod partition;
pub(crate) mod stream;
pub(crate) mod topic;

use crate::args::{partition::PartitionAction, stream::StreamAction, topic::TopicAction};
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
    pub(crate) username: String,

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
}
