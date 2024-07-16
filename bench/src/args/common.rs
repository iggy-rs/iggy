use super::kind::BenchmarkKindCommand;
use super::props::{BenchmarkKindProps, BenchmarkTransportProps};
use super::{defaults::*, transport::BenchmarkTransportCommand};
use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};
use iggy::utils::duration::IggyDuration;
use integration::test_server::Transport;
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct IggyBenchArgs {
    /// Benchmark kind
    #[command(subcommand)]
    pub benchmark_kind: BenchmarkKindCommand,

    /// iggy-server executable path
    #[arg(long, short='e', default_value = None, value_parser = validate_server_executable_path)]
    pub server_executable_path: Option<String>,

    /// Shutdown iggy-server and remove server local_data directory after the benchmark is finished
    #[arg(long, short='c', default_value_t = DEFAULT_PERFORM_CLEANUP)]
    pub cleanup: bool,

    /// Server local data directory path, if not provided a temporary directory will be created
    #[arg(long, short='s', default_value_t = DEFAULT_SERVER_SYSTEM_PATH.to_owned())]
    pub server_system_path: String,

    /// Server stdout visibility
    #[arg(long, short='v', default_value_t = DEFAULT_SERVER_STDOUT_VISIBILITY)]
    pub verbose: bool,

    /// Warmup time
    #[arg(long, short = 'w', default_value_t = IggyDuration::from_str(DEFAULT_WARMUP_TIME).unwrap())]
    pub warmup_time: IggyDuration,

    /// Skip server start
    #[arg(long, short = 'k', default_value_t = DEFAULT_SKIP_SERVER_START)]
    pub skip_server_start: bool,
}

fn validate_server_executable_path(v: &str) -> Result<String, String> {
    if Path::new(v).exists() {
        Ok(v.to_owned())
    } else {
        Err(format!("Provided server executable '{v}' does not exist."))
    }
}

impl IggyBenchArgs {
    pub fn transport_command(&self) -> &BenchmarkTransportCommand {
        self.benchmark_kind.transport_command()
    }

    pub fn transport(&self) -> &Transport {
        self.benchmark_kind.transport_command().transport()
    }

    pub fn server_address(&self) -> &str {
        self.benchmark_kind
            .inner()
            .transport_command()
            .server_address()
    }

    pub fn start_stream_id(&self) -> u32 {
        self.benchmark_kind.transport_command().start_stream_id()
    }

    pub fn validate(&self) {
        let server_address = self.server_address().parse::<SocketAddr>().unwrap();
        if (self.cleanup || self.verbose) && !server_address.ip().is_loopback() {
            IggyBenchArgs::command()
                .error(
                    ErrorKind::ArgumentConflict,
                    format!(
                        "Cannot use cleanup or verbose flags with a non-loopback server address: {}",
                        self.server_address()
                    ),
                )
                .exit();
        }

        self.benchmark_kind.inner().validate()
    }

    pub fn messages_per_batch(&self) -> u32 {
        self.benchmark_kind.inner().messages_per_batch()
    }

    pub fn message_batches(&self) -> u32 {
        self.benchmark_kind.inner().message_batches()
    }

    pub fn message_size(&self) -> u32 {
        self.benchmark_kind.inner().message_size()
    }

    pub fn number_of_streams(&self) -> u32 {
        self.benchmark_kind.inner().number_of_streams()
    }

    pub fn number_of_partitions(&self) -> u32 {
        self.benchmark_kind.inner().number_of_partitions()
    }

    pub fn consumers(&self) -> u32 {
        self.benchmark_kind.inner().consumers()
    }

    pub fn producers(&self) -> u32 {
        self.benchmark_kind.inner().producers()
    }

    pub fn disable_parallel_producer_streams(&self) -> bool {
        self.benchmark_kind
            .inner()
            .disable_parallel_producer_streams()
    }

    pub fn disable_parallel_consumer_streams(&self) -> bool {
        self.benchmark_kind
            .inner()
            .disable_parallel_consumer_streams()
    }

    pub fn number_of_consumer_groups(&self) -> u32 {
        self.benchmark_kind.inner().number_of_consumer_groups()
    }

    pub fn warmup_time(&self) -> IggyDuration {
        self.warmup_time
    }
}
