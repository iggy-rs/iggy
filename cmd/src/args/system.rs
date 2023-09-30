use clap::Args;

#[derive(Debug, Args)]
pub(crate) struct PingArgs {
    /// Stop after sending count Ping packets
    #[arg(short, long, default_value_t = 1)]
    pub(crate) count: u32,
}
