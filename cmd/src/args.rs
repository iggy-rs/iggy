use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

use iggy::args::Args as IggyArgs;

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
}

#[derive(Debug, Subcommand)]
pub(crate) enum StreamAction {
    /// Create stream with given id and name
    Create { id: u32, name: String },
    /// Delete stream with given id
    Delete { id: u32 },
    /// Update stream name for given stream id
    Update { id: u32, name: String },
    /// Delete stream with given id
    Get { id: u32 },
    /// List all streams
    List(ListArgs),
}

#[derive(Debug, Args)]
pub(crate) struct ListArgs {
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum ListMode {
    Table,
    List,
}
