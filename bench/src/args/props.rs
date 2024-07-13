use super::transport::BenchmarkTransportCommand;
use integration::test_server::Transport;

pub trait BenchmarkKindProps {
    fn message_size(&self) -> u32;
    fn messages_per_batch(&self) -> u32;
    fn message_batches(&self) -> u32;
    fn number_of_streams(&self) -> u32;
    fn number_of_partitions(&self) -> u32;
    fn number_of_consumer_groups(&self) -> u32;
    fn consumers(&self) -> u32;
    fn producers(&self) -> u32;
    fn disable_parallel_producer_streams(&self) -> bool;
    fn disable_parallel_consumer_streams(&self) -> bool;
    fn transport_command(&self) -> &BenchmarkTransportCommand;
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
    fn start_stream_id(&self) -> u32;
    fn validate_certificate(&self) -> bool;
    fn inner(&self) -> &dyn BenchmarkTransportProps
    where
        Self: std::marker::Sized,
    {
        self
    }
}
