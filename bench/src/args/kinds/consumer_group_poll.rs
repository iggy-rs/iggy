use crate::args::{
    common::IggyBenchArgs, defaults::*, props::BenchmarkKindProps,
    transport::BenchmarkTransportCommand,
};
use clap::{error::ErrorKind, CommandFactory, Parser};
use iggy::{messages::poll_messages::PollingKind, utils::byte_size::IggyByteSize};
use std::num::NonZeroU32;
use tracing::warn;

/// Polling benchmark with consumer group
#[derive(Parser, Debug, Clone)]
pub struct ConsumerGroupArgs {
    #[command(subcommand)]
    pub transport: BenchmarkTransportCommand,

    /// Polling kind for consumer group poll
    #[arg(long, default_value_t = DEFAULT_POLLING_KIND_CG_POLL)]
    pub polling_kind: PollingKind,

    /// Number of messages per batch
    #[arg(long, default_value_t = DEFAULT_MESSAGES_PER_BATCH)]
    pub messages_per_batch: NonZeroU32,

    /// Number of message batches
    #[arg(long, default_value_t = DEFAULT_MESSAGE_BATCHES)]
    pub message_batches: NonZeroU32,

    /// Message size in bytes
    #[arg(long, default_value_t = DEFAULT_MESSAGE_SIZE)]
    pub message_size: NonZeroU32,

    /// Number of streams
    #[arg(long, default_value_t = DEFAULT_NUMBER_OF_STREAMS_CONSUMER_GROUP)]
    pub streams: NonZeroU32,

    /// Number of consumers
    #[arg(long, default_value_t = DEFAULT_NUMBER_OF_CONSUMERS)]
    pub consumers: NonZeroU32,

    /// Number of consumers
    #[arg(long, default_value_t = DEFAULT_NUMBER_OF_CONSUMER_GROUPS)]
    pub consumer_groups: NonZeroU32,
}

impl BenchmarkKindProps for ConsumerGroupArgs {
    fn message_size(&self) -> u32 {
        self.message_size.get()
    }

    fn messages_per_batch(&self) -> u32 {
        self.messages_per_batch.get()
    }

    fn message_batches(&self) -> u32 {
        self.message_batches.get()
    }

    fn number_of_streams(&self) -> u32 {
        self.streams.get()
    }

    fn number_of_partitions(&self) -> u32 {
        0
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
        false
    }

    fn transport_command(&self) -> &BenchmarkTransportCommand {
        &self.transport
    }

    fn number_of_consumer_groups(&self) -> u32 {
        self.consumer_groups.get()
    }

    fn validate(&self) {
        let cg_number = self.consumer_groups.get();
        let consumers_number = self.consumers.get();
        let mut cmd = IggyBenchArgs::command();
        if cg_number < 1 {
            cmd.error(
                ErrorKind::ArgumentConflict,
                "Consumer groups number must be greater than 0 for a consumer groups benchmark.",
            )
            .exit();
        }

        if consumers_number < 1 {
            cmd.error(
                ErrorKind::ArgumentConflict,
                "Consumers number must be greater than 0 for a consumer groups benchmark.",
            )
            .exit();
        }

        if self.polling_kind != PollingKind::Offset {
            warn!("Polling kind offset for consumer groups benchmark is not recommended.")
        };
    }

    fn polling_kind(&self) -> PollingKind {
        self.polling_kind
    }

    fn max_topic_size(&self) -> Option<IggyByteSize> {
        None
    }
}
