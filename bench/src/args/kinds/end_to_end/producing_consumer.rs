use crate::args::{
    common::IggyBenchArgs, defaults::*, props::BenchmarkKindProps,
    transport::BenchmarkTransportCommand,
};
use clap::{error::ErrorKind, CommandFactory, Parser};
use iggy::utils::byte_size::IggyByteSize;
use std::num::NonZeroU32;

#[derive(Parser, Debug, Clone)]
pub struct EndToEndProducingConsumerArgs {
    #[command(subcommand)]
    pub transport: BenchmarkTransportCommand,

    /// Number of streams
    #[arg(long, short = 's', default_value_t = DEFAULT_PINNED_NUMBER_OF_STREAMS)]
    pub streams: NonZeroU32,

    /// Number of producing consumers
    #[arg(long, short = 'p', default_value_t = DEFAULT_NUMBER_OF_PRODUCERS)]
    pub producers: NonZeroU32,

    /// Max topic size in human readable format, e.g. "1GiB", "2MB", "1GB". If not provided then the server default will be used.
    #[arg(long, short = 't')]
    pub max_topic_size: Option<IggyByteSize>,
}

impl BenchmarkKindProps for EndToEndProducingConsumerArgs {
    fn streams(&self) -> u32 {
        self.streams.get()
    }

    fn partitions(&self) -> u32 {
        1
    }

    fn consumers(&self) -> u32 {
        self.producers.get()
    }

    fn producers(&self) -> u32 {
        self.producers.get()
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
        let mut cmd = IggyBenchArgs::command();
        let streams = self.streams.get();
        let producers = self.producers.get();
        if streams > producers {
            cmd.error(
                ErrorKind::ArgumentConflict,
                format!("For producing consumer, number of streams ({streams}) must be less than or equal to the number of producers ({producers}).",
            ))
            .exit();
        }
    }
}
