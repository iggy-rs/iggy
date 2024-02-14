use crate::args::common::ListMode;
use clap::{Args, Subcommand};

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum ContextAction {
    /// List all contexts
    ///
    /// Examples
    ///  iggy context list
    #[clap(verbatim_doc_comment, visible_alias = "l")]
    List(ContextListArgs),

    /// Set the active context
    ///
    /// Examples
    ///  iggy context use dev
    ///  iggy context use default
    #[clap(verbatim_doc_comment, visible_alias = "u")]
    Use(ContextUseArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ContextListArgs {
    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ContextUseArgs {
    /// Name of the context to use
    #[arg(value_parser = clap::value_parser!(String))]
    pub(crate) context_name: String,
}
