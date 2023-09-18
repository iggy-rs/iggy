pub(crate) mod common;
pub(crate) mod topic;

use crate::args::common::ListMode;
use crate::args::topic::TopicAction;
use clap::{Args, Parser, Subcommand};
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
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// stream operations
    #[clap(subcommand)]
    Stream(StreamAction),
    /// topic operations
    #[clap(subcommand)]
    Topic(TopicAction),
}

#[derive(Debug, Subcommand)]
pub(crate) enum StreamAction {
    /// Create stream with given ID and name
    Create { id: u32, name: String },
    /// Delete stream with given ID
    Delete { id: u32 },
    /// Update stream name for given stream ID
    Update { id: u32, name: String },
    /// Get details of a single stream with given ID
    Get { id: u32 },
    /// List all streams
    List(StreamListArgs),
}

#[derive(Debug, Args)]
pub(crate) struct StreamListArgs {
    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}
