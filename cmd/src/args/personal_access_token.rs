use crate::args::common::ListMode;
use clap::{Args, Subcommand};
use iggy::cmd::utils::personal_access_token_expiry::PersonalAccessTokenExpiry;
use std::convert::From;

#[derive(Debug, Subcommand)]
pub(crate) enum PersonalAccessTokenAction {
    /// Create personal access token
    ///
    /// Create personal access token which allow authenticating the clients using
    /// a token, instead of the regular credentials (username and password)
    /// In quiet mode only the personal access token name is printed
    ///
    /// Examples
    ///  iggy pat create name
    ///  iggy pat create client 1day
    ///  iggy pat create sensor 3weeks
    #[clap(verbatim_doc_comment)]
    Create(PersonalAccessTokenCreateArgs),
    /// Delete personal access token
    ///
    /// Examples
    ///  iggy pat delete name
    ///  iggy pat delete client
    #[clap(verbatim_doc_comment)]
    Delete(PersonalAccessTokenDeleteArgs),
    /// List all personal access tokens
    ///
    /// Examples
    ///  iggy pat list
    #[clap(verbatim_doc_comment)]
    List(PersonalAccessTokenListArgs),
}

#[derive(Debug, Args)]
pub(crate) struct PersonalAccessTokenCreateArgs {
    /// Name of the personal access token
    pub(crate) name: String,
    /// Personal access token expiry time in human readable format
    ///
    /// Expiry time must be expressed in human readable format like 15days 2min 2s
    /// ("none" or skipping parameter disables personal access token expiry)
    #[arg(value_parser = clap::value_parser!(PersonalAccessTokenExpiry))]
    pub(crate) expiry: Option<Vec<PersonalAccessTokenExpiry>>,
}

#[derive(Debug, Args)]
pub(crate) struct PersonalAccessTokenDeleteArgs {
    /// Personal access token name to delete
    pub(crate) name: String,
}

#[derive(Debug, Args)]
pub(crate) struct PersonalAccessTokenListArgs {
    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}
