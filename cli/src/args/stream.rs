use crate::args::common::ListMode;
use clap::{Args, Subcommand};
use iggy::identifier::Identifier;

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum StreamAction {
    /// Create stream with given name
    ///
    /// If stream ID is not provided then the server will automatically assign it
    ///
    /// Examples:
    ///  iggy stream create prod
    ///  iggy stream create -s 1 test
    #[clap(verbatim_doc_comment, visible_alias = "c")]
    Create(StreamCreateArgs),
    /// Delete stream with given ID
    ///
    /// Stream ID can be specified as a stream name or ID
    ///
    /// Examples:
    ///  iggy stream delete 1
    ///  iggy stream delete test
    #[clap(verbatim_doc_comment, visible_alias = "d")]
    Delete(StreamDeleteArgs),
    /// Update stream name for given stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    ///
    /// Examples:
    ///  iggy stream update 1 production
    ///  iggy stream update test development
    #[clap(verbatim_doc_comment, visible_alias = "u")]
    Update(StreamUpdateArgs),
    /// Get details of a single stream with given ID
    ///
    /// Stream ID can be specified as a stream name or ID
    ///
    /// Examples:
    ///  iggy stream get 1
    ///  iggy stream get test
    #[clap(verbatim_doc_comment, visible_alias = "g")]
    Get(StreamGetArgs),
    /// List all streams
    ///
    /// Examples:
    ///  iggy stream list
    ///  iggy stream list --list-mode table
    ///  iggy stream list -l table
    #[clap(verbatim_doc_comment, visible_alias = "l")]
    List(StreamListArgs),
    /// Purge all topics in given stream ID
    ///
    /// Command removes all messages from given stream
    /// Stream ID can be specified as a stream name or ID
    ///
    /// Examples:
    ///  iggy stream purge 1
    ///  iggy stream purge test
    #[clap(verbatim_doc_comment, visible_alias = "p")]
    Purge(StreamPurgeArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct StreamCreateArgs {
    /// Stream ID to create
    #[clap(short, long)]
    pub(crate) stream_id: Option<u32>,
    /// Name of the stream
    pub(crate) name: String,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct StreamDeleteArgs {
    /// Stream ID to delete
    ///
    /// Stream ID can be specified as a stream name or ID
    pub(crate) stream_id: Identifier,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct StreamUpdateArgs {
    /// Stream ID to update
    ///
    /// Stream ID can be specified as a stream name or ID
    pub(crate) stream_id: Identifier,
    /// New name for the stream
    pub(crate) name: String,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct StreamGetArgs {
    /// Stream ID to get
    ///
    /// Stream ID can be specified as a stream name or ID
    pub(crate) stream_id: Identifier,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct StreamListArgs {
    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct StreamPurgeArgs {
    /// Stream ID to purge
    ///
    /// Stream ID can be specified as a stream name or ID
    pub(crate) stream_id: Identifier,
}
