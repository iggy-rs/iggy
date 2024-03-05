use crate::args::common::ListModeExt;
use clap::Args;
use iggy::cli::utils::login_session_expiry::LoginSessionExpiry;

#[derive(Debug, Clone, Args)]
pub(crate) struct PingArgs {
    /// Stop after sending count Ping packets
    #[arg(short, long, default_value_t = 1)]
    pub(crate) count: u32,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct LoginArgs {
    /// Login session expiry time in human-readable format
    ///
    /// Expiry time must be expressed in human-readable format like 1hour 15min 2s.
    /// If not set default value 15minutes is used. Using "none" disables session expiry time.
    #[clap(verbatim_doc_comment)]
    #[arg(value_parser = clap::value_parser!(LoginSessionExpiry), group = "store")]
    pub(crate) expiry: Option<Vec<LoginSessionExpiry>>,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct StatsArgs {
    /// List mode (table, list, JSON, TOML)
    #[clap(short, long, value_enum, default_value_t = ListModeExt::Table)]
    pub(crate) output: ListModeExt,
}
