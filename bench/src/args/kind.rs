use super::examples::print_examples;
use super::kinds::consumer_group_poll::ConsumerGroupArgs;
use super::kinds::poll::PollArgs;
use super::kinds::send::SendArgs;
use super::kinds::send_and_poll::SendAndPollArgs;
use super::props::BenchmarkKindProps;
use super::transport::BenchmarkTransportCommand;
use clap::Subcommand;
use iggy::messages::poll_messages::PollingKind;
use iggy_benchmark_report::benchmark_kind::BenchmarkKind;

#[derive(Subcommand, Debug)]
pub enum BenchmarkKindCommand {
    Send(SendArgs),
    Poll(PollArgs),
    SendAndPoll(SendAndPollArgs),
    ConsumerGroupPoll(ConsumerGroupArgs),

    /// Prints examples
    Examples,
}

impl BenchmarkKindCommand {
    pub fn as_simple_kind(&self) -> BenchmarkKind {
        match self {
            BenchmarkKindCommand::Send(_) => BenchmarkKind::Send,
            BenchmarkKindCommand::Poll(_) => BenchmarkKind::Poll,
            BenchmarkKindCommand::SendAndPoll(_) => BenchmarkKind::SendAndPoll,
            BenchmarkKindCommand::ConsumerGroupPoll(_) => BenchmarkKind::ConsumerGroupPoll,
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

    fn number_of_partitions(&self) -> u32 {
        self.inner().number_of_partitions()
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

    fn number_of_consumer_groups(&self) -> u32 {
        self.inner().number_of_consumer_groups()
    }

    fn polling_kind(&self) -> PollingKind {
        self.inner().polling_kind()
    }

    fn inner(&self) -> &dyn BenchmarkKindProps {
        match self {
            BenchmarkKindCommand::Send(args) => args,
            BenchmarkKindCommand::Poll(args) => args,
            BenchmarkKindCommand::SendAndPoll(args) => args,
            BenchmarkKindCommand::ConsumerGroupPoll(args) => args,
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
