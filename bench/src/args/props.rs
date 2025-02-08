use super::{output::BenchmarkOutputCommand, transport::BenchmarkTransportCommand};
use iggy::utils::byte_size::IggyByteSize;
use integration::test_server::Transport;

pub trait BenchmarkKindProps {
    fn streams(&self) -> u32;
    fn partitions(&self) -> u32;
    fn number_of_consumer_groups(&self) -> u32;
    fn consumers(&self) -> u32;
    fn producers(&self) -> u32;
    fn transport_command(&self) -> &BenchmarkTransportCommand;
    fn max_topic_size(&self) -> Option<IggyByteSize>;
    fn validate(&self);
    fn inner(&self) -> &dyn BenchmarkKindProps
    where
        Self: std::marker::Sized,
    {
        self
    }
}

pub trait BenchmarkTransportProps {
    fn transport(&self) -> &Transport;
    fn server_address(&self) -> &str;
    fn client_address(&self) -> &str;
    fn validate_certificate(&self) -> bool;
    fn output_command(&self) -> &Option<BenchmarkOutputCommand>;
    fn inner(&self) -> &dyn BenchmarkTransportProps
    where
        Self: std::marker::Sized,
    {
        self
    }
}
