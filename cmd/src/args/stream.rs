use crate::args::common::ListMode;
use clap::{Args, Subcommand};
use iggy::identifier::Identifier;

#[derive(Debug, Subcommand)]
pub(crate) enum StreamAction {
    /// Create stream with given ID and name
    ///
    /// Examples:
    ///  iggy stream create 1 prod
    ///  iggy stream create 2 test
    #[clap(verbatim_doc_comment)]
    Create(StreamCreateArgs),
    /// Delete stream with given ID
    ///
    /// Stream ID can be specified as a stream name or ID
    ///
    /// Examples:
    ///  iggy stream delete 1
    ///  iggy stream delete test
    #[clap(verbatim_doc_comment)]
    Delete(StreamDeleteArgs),
    /// Update stream name for given stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    ///
    /// Examples:
    ///  iggy stream update 1 production
    ///  iggy stream update test development
    #[clap(verbatim_doc_comment)]
    Update(StreamUpdateArgs),
    /// Get details of a single stream with given ID
    ///
    /// Stream ID can be specified as a stream name or ID
    ///
    /// Examples:
    ///  iggy stream get 1
    ///  iggy stream get test
    #[clap(verbatim_doc_comment)]
    Get(StreamGetArgs),
    /// List all streams
    ///
    /// Examples:
    ///  iggy stream list
    ///  iggy stream list --list-mode table
    ///  iggy stream list -l table
    #[clap(verbatim_doc_comment)]
    List(StreamListArgs),
}

#[derive(Debug, Args)]
pub(crate) struct StreamCreateArgs {
    /// Stream ID to create topic
    pub(crate) stream_id: u32,
    /// Name of the stream
    pub(crate) name: String,
}

#[derive(Debug, Args)]
pub(crate) struct StreamDeleteArgs {
    /// Stream ID to delete
    ///
    /// Stream ID can be specified as a stream name or ID
    pub(crate) stream_id: Identifier,
}

#[derive(Debug, Args)]
pub(crate) struct StreamUpdateArgs {
    /// Stream ID to update
    ///
    /// Stream ID can be specified as a stream name or ID
    pub(crate) stream_id: Identifier,
    /// New name for the stream
    pub(crate) name: String,
}

#[derive(Debug, Args)]
pub(crate) struct StreamGetArgs {
    /// Stream ID to get
    ///
    /// Stream ID can be specified as a stream name or ID
    pub(crate) stream_id: Identifier,
}

#[derive(Debug, Args)]
pub(crate) struct StreamListArgs {
    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}
