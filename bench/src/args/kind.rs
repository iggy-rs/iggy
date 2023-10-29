use super::defaults::*;
use super::examples::print_examples;
use super::props::BenchmarkKindProps;
use super::transport::BenchmarkTransportCommand;
use super::{common::IggyBenchArgs, simple::BenchmarkKind};
use clap::{error::ErrorKind, CommandFactory, Parser, Subcommand};
use std::num::NonZeroU32;

#[derive(Subcommand, Debug)]
pub enum BenchmarkKindCommand {
    Send(SendArgs),
    Poll(PollArgs),
    SendAndPoll(SendAndPollArgs),

    /// Prints examples
    Examples,
}

impl BenchmarkKindCommand {
    pub fn as_simple_kind(&self) -> BenchmarkKind {
        match self {
            BenchmarkKindCommand::Send(_) => BenchmarkKind::Send,
            BenchmarkKindCommand::Poll(_) => BenchmarkKind::Poll,
            BenchmarkKindCommand::SendAndPoll(_) => BenchmarkKind::SendAndPoll,
            BenchmarkKindCommand::Examples => {
                print_examples();
                std::process::exit(0);
            }
        }
    }
}

impl BenchmarkKindProps for BenchmarkKindCommand {
    fn message_size(&self) -> u32 {
        self.inner().message_size()
    }

    fn messages_per_batch(&self) -> u32 {
        self.inner().messages_per_batch()
    }

    fn message_batches(&self) -> u32 {
        self.inner().message_batches()
    }

    fn number_of_streams(&self) -> u32 {
        self.inner().number_of_streams()
    }

    fn consumers(&self) -> u32 {
        self.inner().consumers()
    }

    fn producers(&self) -> u32 {
        self.inner().producers()
    }

    fn disable_parallel_producer_streams(&self) -> bool {
        self.inner().disable_parallel_producer_streams()
    }

    fn disable_parallel_consumer_streams(&self) -> bool {
        self.inner().disable_parallel_consumer_streams()
    }

    fn transport_command(&self) -> &BenchmarkTransportCommand {
        self.inner().transport_command()
    }

    fn inner(&self) -> &dyn BenchmarkKindProps {
        match self {
            BenchmarkKindCommand::Send(args) => args,
            BenchmarkKindCommand::Poll(args) => args,
            BenchmarkKindCommand::SendAndPoll(args) => args,
            BenchmarkKindCommand::Examples => {
                print_examples();
                std::process::exit(0);
            }
        }
    }

    fn validate(&self) {
        self.inner().validate()
    }
}

/// Sending (writing) benchmark
#[derive(Parser, Debug)]
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

    /// Flag, disables parallel producers
    #[arg(long, default_value_t = DEFAULT_DISABLE_PARALLEL_PRODUCER_STREAMS)]
    pub disable_parallel_producers: bool,
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

    fn consumers(&self) -> u32 {
        panic!("")
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

    fn validate(&self) {
        let streams = self.streams.get();
        let producers = self.producers.get();
        let mut cmd = IggyBenchArgs::command();

        if self.disable_parallel_producers && streams < producers {
            cmd.error(
                ErrorKind::ArgumentConflict,
                format!("With parallel producers flag, the number of streams ({streams}) must be greater than or equal to the number of producers ({producers}).",
            ))
            .exit();
        }
    }
}

/// Polling (reading) benchmark
#[derive(Parser, Debug)]
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

    fn consumers(&self) -> u32 {
        self.consumers.get()
    }

    fn producers(&self) -> u32 {
        panic!("")
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

    fn validate(&self) {
        let streams = self.streams.get();
        let consumers = self.consumers.get();
        let mut cmd = IggyBenchArgs::command();

        if self.disable_parallel_consumers && streams < consumers {
            cmd.error(
                ErrorKind::ArgumentConflict,
                format!("With parallel consumers flag, the number of streams ({streams}) must be greater than or equal to the number of consumers ({consumers})."),
            )
            .exit();
        }
    }
}

/// Parallel sending and polling benchmark
#[derive(Parser, Debug)]
pub struct SendAndPollArgs {
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

    /// Number of consumers
    #[arg(long, default_value_t = DEFAULT_NUMBER_OF_CONSUMERS)]
    pub consumers: NonZeroU32,

    /// Number of streams
    #[arg(long, default_value_t = DEFAULT_NUMBER_OF_STREAMS)]
    pub streams: NonZeroU32,

    /// Flag, disables parallel producers
    #[arg(long, default_value_t = DEFAULT_DISABLE_PARALLEL_PRODUCER_STREAMS)]
    pub disable_parallel_producers: bool,

    /// Flag, disables parallel consumers
    #[arg(long, default_value_t = DEFAULT_DISABLE_PARALLEL_CONSUMER_STREAMS)]
    pub disable_parallel_consumers: bool,
}

impl BenchmarkKindProps for SendAndPollArgs {
    fn message_size(&self) -> u32 {
        self.message_size.get()
    }

    fn number_of_streams(&self) -> u32 {
        self.streams.get()
    }

    fn message_batches(&self) -> u32 {
        self.message_batches.get()
    }

    fn messages_per_batch(&self) -> u32 {
        self.messages_per_batch.get()
    }

    fn consumers(&self) -> u32 {
        self.consumers.get()
    }

    fn producers(&self) -> u32 {
        self.producers.get()
    }

    fn disable_parallel_producer_streams(&self) -> bool {
        self.disable_parallel_producers
    }

    fn disable_parallel_consumer_streams(&self) -> bool {
        self.disable_parallel_consumers
    }

    fn transport_command(&self) -> &BenchmarkTransportCommand {
        &self.transport
    }

    fn validate(&self) {
        let streams = self.streams.get();
        let consumers = self.consumers.get();
        let producers = self.producers.get();
        let mut cmd = IggyBenchArgs::command();

        if self.disable_parallel_consumers && streams < consumers {
            cmd.error(
                ErrorKind::ArgumentConflict,
                format!("With parallel consumers flag, the number of streams ({streams}) must be greater than or equal to the number of consumers ({consumers})."),
            )
            .exit();
        }

        if self.disable_parallel_producers && streams < producers {
            cmd.error(
                ErrorKind::ArgumentConflict,
                format!("With parallel producers flag, the number of streams ({streams}) must be greater than or equal to the number of producers ({producers}).",
            ))
            .exit();
        }
    }
}
