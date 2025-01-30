use crate::args::{
    common::IggyBenchArgs, defaults::*, props::BenchmarkKindProps,
    transport::BenchmarkTransportCommand,
};
use clap::{error::ErrorKind, CommandFactory, Parser};
use iggy::{messages::poll_messages::PollingKind, utils::byte_size::IggyByteSize};
use std::num::NonZeroU32;

/// Sending (writing) benchmark
#[derive(Parser, Debug, Clone)]
pub struct SendArgs {
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

    /// Number of producers
    #[arg(long, default_value_t = DEFAULT_NUMBER_OF_PRODUCERS)]
    pub producers: NonZeroU32,

    /// Number of streams
    #[arg(long, default_value_t = DEFAULT_NUMBER_OF_STREAMS)]
    pub streams: NonZeroU32,

    /// Number of partitions
    #[arg(long, default_value_t = DEFAULT_NUMBER_OF_PARTITIONS)]
    pub partitions: NonZeroU32,

    /// Flag, disables parallel producers
    #[arg(long, default_value_t = DEFAULT_DISABLE_PARALLEL_PRODUCER_STREAMS)]
    pub disable_parallel_producers: bool,

    /// Max topic size in human readable format, e.g. "1GiB", "2MB", "1GB". If not provided then the server default will be used.
    #[arg(long)]
    pub max_topic_size: Option<IggyByteSize>,
}

impl BenchmarkKindProps for SendArgs {
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
        0
    }

    fn producers(&self) -> u32 {
        self.producers.get()
    }

    fn disable_parallel_producer_streams(&self) -> bool {
        self.disable_parallel_producers
    }

    fn disable_parallel_consumer_streams(&self) -> bool {
        false
    }

    fn transport_command(&self) -> &BenchmarkTransportCommand {
        &self.transport
    }

    fn number_of_consumer_groups(&self) -> u32 {
        0
    }

    fn max_topic_size(&self) -> Option<IggyByteSize> {
        self.max_topic_size
    }

    fn validate(&self) {
        let streams = self.streams.get();
        let producers = self.producers.get();
        let mut cmd = IggyBenchArgs::command();

        if !self.disable_parallel_producers && streams < producers {
            cmd.error(
                ErrorKind::ArgumentConflict,
                format!("Without parallel producers flag, the number of streams ({streams}) must be greater than or equal to the number of producers ({producers}).",
            ))
            .exit();
        }
    }

    fn polling_kind(&self) -> PollingKind {
        panic!("Polling kind selection is not supported for send benchmark")
    }
}
