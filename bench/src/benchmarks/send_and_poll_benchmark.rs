use super::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::actors::consumer::Consumer;
use crate::actors::producer::Producer;
use crate::args::common::IggyBenchArgs;
use async_trait::async_trait;
use iggy::messages::poll_messages::PollingKind;
use iggy_benchmark_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct SendAndPollMessagesBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl SendAndPollMessagesBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for SendAndPollMessagesBenchmark {
    async fn run(&mut self) -> BenchmarkFutures {
        self.init_streams().await.expect("Failed to init streams!");
        let start_stream_id = self.args.start_stream_id();
        let producers = self.args.producers();
        let consumers = self.args.consumers();
        let parallel_producer_streams = !self.args.disable_parallel_producer_streams();
        let parallel_consumer_streams = !self.args.disable_parallel_consumer_streams();
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();
        let message_size = self.args.message_size();
        let partitions_count = self.args.number_of_partitions();
        let warmup_time = self.args.warmup_time();

        let mut futures: BenchmarkFutures =
            Ok(Vec::with_capacity((producers + consumers) as usize));
        for producer_id in 1..=producers {
            let stream_id = match parallel_producer_streams {
                true => start_stream_id + producer_id,
                false => start_stream_id + 1,
            };
            let producer = Producer::new(
                self.client_factory.clone(),
                producer_id,
                stream_id,
                partitions_count,
                messages_per_batch,
                message_batches,
                message_size,
                warmup_time,
                self.args.sampling_time(),
                self.args.moving_average_window(),
            );
            let future = Box::pin(async move { producer.run().await });
            futures.as_mut().unwrap().push(future);
        }

        for consumer_id in 1..=consumers {
            let stream_id = match parallel_consumer_streams {
                true => start_stream_id + consumer_id,
                false => start_stream_id + 1,
            };
            let consumer = Consumer::new(
                self.client_factory.clone(),
                consumer_id,
                None,
                stream_id,
                messages_per_batch,
                message_batches,
                warmup_time,
                self.args.sampling_time(),
                self.args.moving_average_window(),
                PollingKind::Offset,
            );
            let future = Box::pin(async move { consumer.run().await });
            futures.as_mut().unwrap().push(future);
        }
        info!(
            "Starting to send and poll {} messages",
            self.total_messages()
        );
        futures
    }

    fn kind(&self) -> BenchmarkKind {
        BenchmarkKind::SendAndPoll
    }

    fn args(&self) -> &IggyBenchArgs {
        &self.args
    }

    fn client_factory(&self) -> &Arc<dyn ClientFactory> {
        &self.client_factory
    }
}
