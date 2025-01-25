use super::kind::BenchmarkKindCommand;
use super::props::{BenchmarkKindProps, BenchmarkTransportProps};
use super::{defaults::*, transport::BenchmarkTransportCommand};
use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};
use iggy::utils::duration::IggyDuration;
use iggy_benchmark_report::benchmark_kind::BenchmarkKind;
use iggy_benchmark_report::params::BenchmarkParams;
use iggy_benchmark_report::transport::BenchmarkTransport;
use integration::test_server::Transport;
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use tracing::info;

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

    /// Sampling time for metrics collection. It is also used as bucket size for time series calculations.
    #[arg(long, default_value_t = IggyDuration::from_str(DEFAULT_SAMPLING_TIME).unwrap(), value_parser = IggyDuration::from_str)]
    pub sampling_time: IggyDuration,

    /// Window size for moving average calculations in time series data
    #[arg(long, default_value_t = DEFAULT_MOVING_AVERAGE_WINDOW)]
    pub moving_average_window: u32,

    /// Skip server start
    #[arg(long, short = 'k', default_value_t = DEFAULT_SKIP_SERVER_START)]
    pub skip_server_start: bool,

    /// Output directory path for storing benchmark results
    #[arg(long)]
    pub output_dir: Option<String>,

    /// Identifier for the benchmark run (e.g., machine name)
    #[arg(long)]
    pub identifier: Option<String>,

    /// Additional remark for the benchmark (e.g., no-cache)
    #[arg(long)]
    pub remark: Option<String>,

    /// Extra information for future use
    #[arg(long)]
    pub extra_info: Option<String>,

    /// Git reference (commit hash, branch or tag) used for note in the benchmark results
    #[arg(long)]
    pub gitref: Option<String>,

    /// Git reference date used for note in the benchmark results, preferably merge date of the commit
    #[arg(long)]
    pub gitref_date: Option<String>,
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

        if self.output_dir.is_none()
            && (self.gitref.is_some()
                || self.identifier.is_some()
                || self.remark.is_some()
                || self.extra_info.is_some()
                || self.gitref_date.is_some())
        {
            IggyBenchArgs::command()
                .error(
                    ErrorKind::ArgumentConflict,
                    "--git-ref, --git-ref-date, --identifier, --remark, --extra-info can only be used with --output-dir",
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

    pub fn sampling_time(&self) -> IggyDuration {
        self.sampling_time
    }

    pub fn moving_average_window(&self) -> u32 {
        self.moving_average_window
    }

    pub fn output_dir(&self) -> Option<String> {
        self.output_dir.clone()
    }

    pub fn identifier(&self) -> Option<String> {
        self.identifier.clone()
    }

    pub fn remark(&self) -> Option<String> {
        self.remark.clone()
    }

    pub fn extra_info(&self) -> Option<String> {
        self.extra_info.clone()
    }

    pub fn gitref(&self) -> Option<String> {
        self.gitref.clone()
    }

    pub fn gitref_date(&self) -> Option<String> {
        self.gitref_date.clone()
    }

    /// Generates the output directory name based on benchmark parameters.
    pub fn generate_dir_name(&self) -> String {
        let benchmark_kind = match &self.benchmark_kind {
            BenchmarkKindCommand::Send(_) => "send",
            BenchmarkKindCommand::Poll(_) => "poll",
            BenchmarkKindCommand::SendAndPoll(_) => "send_and_poll",
            BenchmarkKindCommand::ConsumerGroupPoll(_) => "consumer_group_poll",
            BenchmarkKindCommand::Examples => unreachable!(),
        };

        let transport = match self.transport_command() {
            BenchmarkTransportCommand::Tcp(_) => "tcp",
            BenchmarkTransportCommand::Quic(_) => "quic",
            BenchmarkTransportCommand::Http(_) => "http",
        };

        let mut parts = vec![
            benchmark_kind.to_string(),
            match benchmark_kind {
                "send" => self.producers().to_string(),
                _ => self.consumers().to_string(),
            },
            self.message_size().to_string(),
            self.messages_per_batch().to_string(),
            self.message_batches().to_string(),
            transport.to_string(),
        ];

        if let Some(remark) = &self.remark {
            parts.push(remark.to_string());
        }

        if let Some(gitref) = &self.gitref {
            parts.push(gitref.to_string());
        }

        if let Some(identifier) = &self.identifier {
            parts.push(identifier.to_string());
        }

        parts.join("_")
    }

    /// Generates a human-readable pretty name for the benchmark
    pub fn generate_pretty_name(&self) -> String {
        let consumer_or_producer = match &self.benchmark_kind {
            BenchmarkKindCommand::Send(_) => format!("{} producers", self.producers()),
            _ => format!("{} consumers", self.consumers()),
        };

        let mut name = format!(
            "{}, {}B msgs, {} msgs/batch",
            consumer_or_producer,
            self.message_size(),
            self.messages_per_batch(),
        );

        if let Some(remark) = &self.remark {
            name.push_str(&format!(" ({})", remark));
        }

        name
    }
}

fn recreate_bench_command(args: &IggyBenchArgs) -> String {
    let mut parts = Vec::new();

    // If using localhost, add env vars
    let server_address = args.server_address();
    let is_localhost = server_address
        .split(':')
        .next()
        .map(|host| host == "localhost" || host == "127.0.0.1")
        .unwrap_or(false);

    if is_localhost {
        // Get all env vars starting with IGGY_
        let iggy_vars: Vec<_> = std::env::vars()
            .filter(|(k, _)| k.starts_with("IGGY_"))
            .collect();

        if !iggy_vars.is_empty() {
            info!("Found env vars starting with IGGY_: {:?}", iggy_vars);
            parts.extend(iggy_vars.into_iter().map(|(k, v)| format!("{}={}", k, v)));
        }
    }

    parts.push("iggy-bench".to_string());

    // Add optional global args
    if let Some(ref remark) = args.remark() {
        parts.push(format!("--remark \'{}\'", remark));
    }

    if let Some(ref output_dir) = args.output_dir() {
        parts.push(format!("--output-dir \'{}\'", output_dir));
    }

    // Add warmup time if not default
    if args.warmup_time().to_string() != DEFAULT_WARMUP_TIME {
        parts.push(format!("--warmup-time {}", args.warmup_time()));
    }

    // Add benchmark kind
    let kind_str = match args.benchmark_kind.as_simple_kind() {
        BenchmarkKind::Send => "send",
        BenchmarkKind::Poll => "poll",
        BenchmarkKind::SendAndPoll => "send-and-poll",
        BenchmarkKind::ConsumerGroupPoll => "consumer-group-poll",
    };
    parts.push(kind_str.to_string());

    // Add benchmark params, skipping defaults
    let producers = args.producers();
    let consumers = args.consumers();

    match args.benchmark_kind.as_simple_kind() {
        BenchmarkKind::Send => {
            if producers != DEFAULT_NUMBER_OF_PRODUCERS.get() {
                parts.push(format!("--producers {}", producers));
            }
        }
        BenchmarkKind::Poll | BenchmarkKind::ConsumerGroupPoll => {
            if consumers != DEFAULT_NUMBER_OF_CONSUMERS.get() {
                parts.push(format!("--consumers {}", consumers));
            }
        }
        BenchmarkKind::SendAndPoll => {
            if producers != DEFAULT_NUMBER_OF_PRODUCERS.get() {
                parts.push(format!("--producers {}", producers));
            }
            if consumers != DEFAULT_NUMBER_OF_CONSUMERS.get() {
                parts.push(format!("--consumers {}", consumers));
            }
        }
    }

    let messages_per_batch = args.messages_per_batch();
    if messages_per_batch != DEFAULT_MESSAGES_PER_BATCH.get() {
        parts.push(format!("--messages-per-batch {}", messages_per_batch));
    }

    let message_batches = args.message_batches();
    if message_batches != DEFAULT_MESSAGE_BATCHES.get() {
        parts.push(format!("--message-batches {}", message_batches));
    }

    let message_size = args.message_size();
    if message_size != DEFAULT_MESSAGE_SIZE.get() {
        parts.push(format!("--message-size {}", message_size));
    }

    let streams = args.number_of_streams();
    let default_streams = match args.benchmark_kind.as_simple_kind() {
        BenchmarkKind::ConsumerGroupPoll => DEFAULT_NUMBER_OF_STREAMS_CONSUMER_GROUP.get(),
        _ => DEFAULT_NUMBER_OF_STREAMS.get(),
    };
    if streams != default_streams {
        parts.push(format!("--streams {}", streams));
    }

    let partitions = args.number_of_partitions();
    if partitions != DEFAULT_NUMBER_OF_PARTITIONS.get() {
        parts.push(format!("--partitions {}", partitions));
    }

    // Add transport and server address, skipping if default
    let transport = args.transport().to_string().to_lowercase();
    parts.push(transport.clone());

    let default_address = match transport.as_str() {
        "tcp" => DEFAULT_TCP_SERVER_ADDRESS,
        "quic" => DEFAULT_QUIC_SERVER_ADDRESS,
        "http" => DEFAULT_HTTP_SERVER_ADDRESS,
        _ => "",
    };
    if server_address != default_address {
        parts.push(format!("--server-address {}", server_address));
    }

    // Add optional flags, skipping defaults
    if args.disable_parallel_producer_streams() != DEFAULT_DISABLE_PARALLEL_PRODUCER_STREAMS {
        parts.push("--disable-parallel-producer-streams".to_string());
    }
    if args.disable_parallel_consumer_streams() != DEFAULT_DISABLE_PARALLEL_CONSUMER_STREAMS {
        parts.push("--disable-parallel-consumer-streams".to_string());
    }

    // Only add consumer groups for consumer group poll
    let consumer_groups = args.number_of_consumer_groups();
    if args.benchmark_kind.as_simple_kind() == BenchmarkKind::ConsumerGroupPoll
        && consumer_groups != DEFAULT_NUMBER_OF_CONSUMER_GROUPS.get()
    {
        parts.push(format!("--consumer-groups {}", consumer_groups));
    }

    parts.join(" ")
}

impl From<&IggyBenchArgs> for BenchmarkParams {
    fn from(args: &IggyBenchArgs) -> Self {
        let benchmark_kind = args.benchmark_kind.as_simple_kind();

        // Ugly conversion but let it stay here to not have dependencies on other modules
        let transport = match args.transport() {
            Transport::Tcp => BenchmarkTransport::Tcp,
            Transport::Quic => BenchmarkTransport::Quic,
            Transport::Http => BenchmarkTransport::Http,
        };
        let server_address = args.server_address().to_string();
        let remark = args.remark();
        let extra_info = args.extra_info();
        let gitref = args.gitref();
        let gitref_date = args.gitref_date();
        let messages_per_batch = args.messages_per_batch();
        let message_batches = args.message_batches();
        let message_size = args.message_size();
        let producers = args.producers();
        let consumers = args.consumers();
        let streams = args.number_of_streams();
        let partitions = args.number_of_partitions();
        let number_of_consumer_groups = args.number_of_consumer_groups();
        let disable_parallel_consumers = args.disable_parallel_consumer_streams();
        let disable_parallel_producers = args.disable_parallel_producer_streams();
        let pretty_name = args.generate_pretty_name();
        let bench_command = recreate_bench_command(args);

        let remark_for_identifier = remark
            .clone()
            .unwrap_or("no_remark".to_string())
            .replace(' ', "_");

        let params_identifier = vec![
            benchmark_kind.to_string(),
            transport.to_string(),
            remark_for_identifier,
            messages_per_batch.to_string(),
            message_batches.to_string(),
            message_size.to_string(),
            producers.to_string(),
            consumers.to_string(),
            streams.to_string(),
            partitions.to_string(),
            number_of_consumer_groups.to_string(),
        ];

        let params_identifier = params_identifier.join("_");

        BenchmarkParams {
            benchmark_kind,
            transport,
            server_address,
            remark,
            extra_info,
            gitref,
            gitref_date,
            messages_per_batch,
            message_batches,
            message_size,
            producers,
            consumers,
            streams,
            partitions,
            number_of_consumer_groups,
            disable_parallel_consumers,
            disable_parallel_producers,
            pretty_name,
            bench_command,
            params_identifier,
        }
    }
}
