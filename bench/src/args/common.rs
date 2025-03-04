use super::kind::BenchmarkKindCommand;
use super::output::BenchmarkOutputCommand;
use super::props::{BenchmarkKindProps, BenchmarkTransportProps};
use super::{defaults::*, transport::BenchmarkTransportCommand};
use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::duration::IggyDuration;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use iggy_bench_report::numeric_parameter::IggyBenchNumericParameter;
use integration::test_server::Transport;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::Path;
use std::str::FromStr;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct IggyBenchArgs {
    /// Benchmark kind
    #[command(subcommand)]
    pub benchmark_kind: BenchmarkKindCommand,

    /// Message size in bytes. Accepts either a single value or a range (e.g. "200..500")
    #[arg(long, short = 'm', value_parser = IggyBenchNumericParameter::from_str, default_value_t = IggyBenchNumericParameter::Value(DEFAULT_MESSAGE_SIZE.get()))]
    pub message_size: IggyBenchNumericParameter,

    /// Number of messages per batch
    #[arg(long, short = 'p', value_parser = IggyBenchNumericParameter::from_str, default_value_t = IggyBenchNumericParameter::Value(DEFAULT_MESSAGES_PER_BATCH.get()))]
    pub messages_per_batch: IggyBenchNumericParameter,

    /// Number of message batches per actor (producer / consumer / producing consumer).
    /// This argument is mutually exclusive with `total_messages_size`.
    #[arg(long, short = 'b', group = "data_to_process")]
    pub message_batches: Option<NonZeroU32>,

    /// Total size of all messages to process in bytes (aggregate, for all actors)
    /// This argument is mutually exclusive with `message_batches`.
    #[arg(long, short = 't', group = "data_to_process")]
    pub total_data: Option<IggyByteSize>,

    /// Start stream id
    #[arg(long, short = 'S', default_value_t = DEFAULT_START_STREAM_ID)]
    pub start_stream_id: NonZeroU32,

    /// Optional total rate limit (aggregate, for all actors)
    /// Accepts human-readable formats like "50KB", "10MB", or "1GB"
    #[arg(long, short = 'r', verbatim_doc_comment)]
    pub rate_limit: Option<IggyByteSize>,

    /// Warmup time in human readable format, e.g. "1s", "2m", "3h"
    #[arg(long, short = 'w', default_value_t = IggyDuration::from_str(DEFAULT_WARMUP_TIME).unwrap())]
    pub warmup_time: IggyDuration,

    /// Server stdout visibility
    #[arg(long, short = 'v', default_value_t = DEFAULT_SERVER_STDOUT_VISIBILITY)]
    pub verbose: bool,

    /// Sampling time for metrics collection. It is also used as bucket size for time series calculations.
    #[arg(long, short = 'T', default_value_t = IggyDuration::from_str(DEFAULT_SAMPLING_TIME).unwrap(), value_parser = IggyDuration::from_str)]
    pub sampling_time: IggyDuration,

    /// Window size for moving average calculations in time series data
    #[arg(long, short = 'W', default_value_t = DEFAULT_MOVING_AVERAGE_WINDOW)]
    pub moving_average_window: u32,

    /// Shutdown iggy-server and remove server local_data directory after the benchmark is finished.
    /// Only applicable to local benchmarks.
    #[arg(long, default_value_t = DEFAULT_PERFORM_CLEANUP, verbatim_doc_comment)]
    pub cleanup: bool,

    /// iggy-server executable path.
    /// Only applicable to local benchmarks.
    #[arg(long, short='e', default_value = None, value_parser = validate_server_executable_path)]
    pub server_executable_path: Option<String>,

    /// Skip server start.
    /// Only applicable to local benchmarks.
    #[arg(long, short = 'k', default_value_t = DEFAULT_SKIP_SERVER_START, verbatim_doc_comment)]
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

    pub fn nodelay(&self) -> bool {
        self.benchmark_kind.transport_command().nodelay()
    }

    pub fn server_address(&self) -> &str {
        self.benchmark_kind
            .inner()
            .transport_command()
            .server_address()
    }

    pub fn start_stream_id(&self) -> u32 {
        self.start_stream_id.get()
    }

    pub fn validate(&mut self) {
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

        if self.output_dir().is_none()
            && (self.gitref().is_some()
                || self.identifier().is_some()
                || self.remark().is_some()
                || self.extra_info().is_some()
                || self.gitref_date().is_some())
        {
            IggyBenchArgs::command()
                .error(
                    ErrorKind::ArgumentConflict,
                    "--git-ref, --git-ref-date, --identifier, --remark, --extra-info can only be used with --output-dir",
                )
                .exit();
        }

        if let (None, None) = (self.message_batches, self.total_data) {
            self.message_batches = Some(DEFAULT_MESSAGE_BATCHES);
        }

        if let Some(total_data) = self.total_data {
            let samples = total_data.as_bytes_u64() / self.message_size().min() as u64;
            if samples <= 1 {
                IggyBenchArgs::command()
                    .error(
                        ErrorKind::ArgumentConflict,
                        "--total-messages-size must be at least 2x greater than --message-size",
                    )
                    .exit();
            }
        }

        self.benchmark_kind.inner().validate()
    }

    pub fn messages_per_batch(&self) -> IggyBenchNumericParameter {
        self.messages_per_batch
    }

    pub fn message_batches(&self) -> Option<NonZeroU32> {
        self.message_batches
    }

    pub fn message_size(&self) -> IggyBenchNumericParameter {
        self.message_size
    }

    pub fn total_data(&self) -> Option<IggyByteSize> {
        self.total_data
    }

    // Used only for generation of unique directory name
    pub fn data_volume_identifier(&self) -> String {
        if let Some(total_messages_size) = self.total_data() {
            format!("{}B", total_messages_size.as_bytes_u64())
        } else {
            self.message_batches().unwrap().to_string()
        }
    }

    pub fn streams(&self) -> u32 {
        self.benchmark_kind.inner().streams()
    }

    pub fn number_of_partitions(&self) -> u32 {
        self.benchmark_kind.inner().partitions()
    }

    pub fn consumers(&self) -> u32 {
        self.benchmark_kind.inner().consumers()
    }

    pub fn producers(&self) -> u32 {
        self.benchmark_kind.inner().producers()
    }

    pub fn kind(&self) -> BenchmarkKind {
        self.benchmark_kind.as_simple_kind()
    }

    pub fn number_of_consumer_groups(&self) -> u32 {
        self.benchmark_kind.inner().number_of_consumer_groups()
    }

    pub fn warmup_time(&self) -> IggyDuration {
        self.warmup_time
    }

    pub fn sampling_time(&self) -> IggyDuration {
        self.sampling_time
    }

    pub fn moving_average_window(&self) -> u32 {
        self.moving_average_window
    }

    pub fn rate_limit(&self) -> Option<IggyByteSize> {
        self.rate_limit
    }

    pub fn output_dir(&self) -> Option<String> {
        self.benchmark_kind
            .inner()
            .transport_command()
            .output_command()
            .as_ref()
            .map(|cmd| match cmd {
                BenchmarkOutputCommand::Output(args) => args.output_dir.clone(),
            })
    }

    pub fn identifier(&self) -> Option<String> {
        self.benchmark_kind
            .inner()
            .transport_command()
            .output_command()
            .as_ref()
            .map(|cmd| match cmd {
                BenchmarkOutputCommand::Output(args) => args.identifier.clone(),
            })
    }

    pub fn remark(&self) -> Option<String> {
        self.benchmark_kind
            .inner()
            .transport_command()
            .output_command()
            .as_ref()
            .and_then(|cmd| match cmd {
                BenchmarkOutputCommand::Output(args) => args.remark.clone(),
            })
    }

    pub fn extra_info(&self) -> Option<String> {
        self.benchmark_kind
            .inner()
            .transport_command()
            .output_command()
            .as_ref()
            .and_then(|cmd| match cmd {
                BenchmarkOutputCommand::Output(args) => args.extra_info.clone(),
            })
    }

    pub fn gitref(&self) -> Option<String> {
        self.benchmark_kind
            .inner()
            .transport_command()
            .output_command()
            .as_ref()
            .and_then(|cmd| match cmd {
                BenchmarkOutputCommand::Output(args) => args.gitref.clone(),
            })
    }

    pub fn gitref_date(&self) -> Option<String> {
        self.benchmark_kind
            .inner()
            .transport_command()
            .output_command()
            .as_ref()
            .and_then(|cmd| match cmd {
                BenchmarkOutputCommand::Output(args) => args.gitref_date.clone(),
            })
    }

    pub fn open_charts(&self) -> bool {
        self.benchmark_kind
            .inner()
            .transport_command()
            .output_command()
            .as_ref()
            .is_some_and(|cmd| match cmd {
                BenchmarkOutputCommand::Output(args) => args.open_charts,
            })
    }

    pub fn max_topic_size(&self) -> Option<IggyByteSize> {
        self.benchmark_kind.inner().max_topic_size()
    }

    /// Generates the output directory name based on benchmark parameters.
    pub fn generate_dir_name(&self) -> String {
        let benchmark_kind = match &self.benchmark_kind {
            BenchmarkKindCommand::PinnedProducer(_) => "pinned_producer",
            BenchmarkKindCommand::PinnedConsumer(_) => "pinned_consumer",
            BenchmarkKindCommand::PinnedProducerAndConsumer(_) => "pinned_producer_and_consumer",
            BenchmarkKindCommand::BalancedProducer(_) => "balanced_producer",
            BenchmarkKindCommand::BalancedConsumerGroup(_) => "balanced_consumer_group",
            BenchmarkKindCommand::BalancedProducerAndConsumerGroup(_) => {
                "balanced_producer_and_consumer"
            }
            BenchmarkKindCommand::EndToEndProducingConsumer(_) => "end_to_end_producing_consumer",
            BenchmarkKindCommand::EndToEndProducingConsumerGroup(_) => {
                "end_to_end_producing_consumer_group"
            }
            BenchmarkKindCommand::Examples => unreachable!(),
        };

        let transport = match self.transport_command() {
            BenchmarkTransportCommand::Tcp(_) => "tcp",
            BenchmarkTransportCommand::Quic(_) => "quic",
            BenchmarkTransportCommand::Http(_) => "http",
        };

        let actors = match &self.benchmark_kind {
            BenchmarkKindCommand::PinnedProducer(_) => self.producers(),
            BenchmarkKindCommand::PinnedConsumer(_) => self.consumers(),
            BenchmarkKindCommand::PinnedProducerAndConsumer(_) => {
                self.producers() + self.consumers()
            }
            BenchmarkKindCommand::BalancedProducer(_) => self.producers(),
            BenchmarkKindCommand::BalancedConsumerGroup(_) => self.consumers(),
            BenchmarkKindCommand::BalancedProducerAndConsumerGroup(_) => {
                self.producers() + self.consumers()
            }
            BenchmarkKindCommand::EndToEndProducingConsumer(_) => self.producers(),
            BenchmarkKindCommand::EndToEndProducingConsumerGroup(_) => self.producers(),
            BenchmarkKindCommand::Examples => unreachable!(),
        };

        let data_volume_arg = match (self.total_data, self.message_batches) {
            (Some(total), None) => format!("{}", total),
            (None, Some(batches)) => format!("{}", batches),
            _ => unreachable!(),
        };

        let mut parts = vec![
            benchmark_kind.to_string(),
            actors.to_string(),
            self.message_size().to_string(),
            self.messages_per_batch().to_string(),
            data_volume_arg,
            transport.to_string(),
        ];

        if let Some(remark) = &self.remark() {
            parts.push(remark.to_string());
        }

        if let Some(gitref) = &self.gitref() {
            parts.push(gitref.to_string());
        }

        if let Some(identifier) = &self.identifier() {
            parts.push(identifier.to_string());
        }

        parts.join("_")
    }

    /// Generates a human-readable pretty name for the benchmark
    pub fn generate_pretty_name(&self) -> String {
        let consumer_or_producer = match &self.benchmark_kind {
            BenchmarkKindCommand::PinnedProducer(_) | BenchmarkKindCommand::BalancedProducer(_) => {
                format!("{} producers", self.producers())
            }
            BenchmarkKindCommand::PinnedConsumer(_)
            | BenchmarkKindCommand::BalancedConsumerGroup(_) => {
                format!("{} consumers", self.consumers())
            }
            BenchmarkKindCommand::PinnedProducerAndConsumer(_)
            | BenchmarkKindCommand::BalancedProducerAndConsumerGroup(_) => format!(
                "{} producers/{} consumers",
                self.producers(),
                self.consumers()
            ),
            BenchmarkKindCommand::EndToEndProducingConsumer(_) => {
                format!("{} producing consumers", self.producers(),)
            }
            BenchmarkKindCommand::EndToEndProducingConsumerGroup(_) => {
                format!(
                    "{} producing consumers/{} consumer groups",
                    self.producers(),
                    self.consumers()
                )
            }
            BenchmarkKindCommand::Examples => unreachable!(),
        };

        let mut name = format!(
            "{}, {}B msgs, {} msgs/batch",
            consumer_or_producer,
            self.message_size(),
            self.messages_per_batch(),
        );

        if let Some(remark) = &self.remark() {
            name.push_str(&format!(" ({})", remark));
        }

        name
    }
}
