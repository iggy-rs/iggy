use crate::args::{
    common::IggyBenchArgs, defaults::*, props::BenchmarkKindProps,
    transport::BenchmarkTransportCommand,
};
use clap::{error::ErrorKind, CommandFactory, Parser};
use iggy::messages::poll_messages::PollingKind;
use std::num::NonZeroU32;

/// Polling (reading) benchmark
#[derive(Parser, Debug, Clone)]
pub struct PollArgs {
    #[command(subcommand)]
    pub transport: BenchmarkTransportCommand,

    /// Number of messages per batch
    #[arg(long, default_value_t = DEFAULT_MESSAGES_PER_BATCH)]
    pub messages_per_batch: NonZeroU32,

    /// Number of message batches
    #[arg(long, default_value_t = DEFAULT_MESSAGE_BATCHES)]
    pub message_batches: NonZeroU32,

    /// Message size in bytes
    #[arg(long, default_value_t = DEFAULT_MESSAGE_SIZE)]
    pub message_size: NonZeroU32,

    /// Number of consumers
    #[arg(long, default_value_t = DEFAULT_NUMBER_OF_CONSUMERS)]
    pub consumers: NonZeroU32,

    /// Number of streams
    #[arg(long, default_value_t = DEFAULT_NUMBER_OF_STREAMS)]
    pub streams: NonZeroU32,

    /// Number of streams
    #[arg(long, default_value_t = DEFAULT_NUMBER_OF_PARTITIONS)]
    pub partitions: NonZeroU32,

    /// Flag, disables parallel consumers
    #[arg(long, default_value_t = DEFAULT_DISABLE_PARALLEL_CONSUMER_STREAMS)]
    pub disable_parallel_consumers: bool,
}

impl BenchmarkKindProps for PollArgs {
    fn message_size(&self) -> u32 {
        self.message_size.get()
    }

    fn message_batches(&self) -> u32 {
        self.message_batches.get()
    }

    fn messages_per_batch(&self) -> u32 {
        self.messages_per_batch.get()
    }

    fn number_of_streams(&self) -> u32 {
        self.streams.get()
    }

    fn number_of_partitions(&self) -> u32 {
        self.partitions.get()
    }

    fn consumers(&self) -> u32 {
        self.consumers.get()
    }

    fn producers(&self) -> u32 {
        0
    }

    fn disable_parallel_producer_streams(&self) -> bool {
        false
    }

    fn disable_parallel_consumer_streams(&self) -> bool {
        self.disable_parallel_consumers
    }

    fn transport_command(&self) -> &BenchmarkTransportCommand {
        &self.transport
    }

    fn number_of_consumer_groups(&self) -> u32 {
        0
    }

    fn validate(&self) {
        let streams = self.streams.get();
        let consumers = self.consumers.get();
        let mut cmd = IggyBenchArgs::command();

        if !self.disable_parallel_consumers && streams < consumers {
            cmd.error(
                ErrorKind::ArgumentConflict,
                format!("Without parallel consumers flag, the number of streams ({streams}) must be greater than or equal to the number of consumers ({consumers})."),
            )
            .exit();
        }
    }

    fn polling_kind(&self) -> PollingKind {
        PollingKind::Offset
    }
}
