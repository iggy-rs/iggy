use crate::args::common::ListMode;
use clap::{Args, Subcommand};

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum ClientAction {
    /// Get details of a single client with given ID
    ///
    /// Client ID is unique numerical identifier not to be confused with the user.
    ///
    /// Examples:
    ///  iggy client get 42
    #[clap(verbatim_doc_comment, visible_alias = "g")]
    Get(ClientGetArgs),
    /// List all currently connected clients to iggy server
    ///
    /// Clients shall not to be confused with the users
    ///
    /// Examples:
    ///  iggy client list
    ///  iggy client list --list-mode table
    ///  iggy client list -l table
    #[clap(verbatim_doc_comment, visible_alias = "l")]
    List(ClientListArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ClientGetArgs {
    /// Client ID to get
    pub(crate) client_id: u32,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ClientListArgs {
    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}
