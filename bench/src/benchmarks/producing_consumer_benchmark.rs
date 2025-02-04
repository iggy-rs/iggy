use crate::actors::producing_consumer::ProducingConsumer;
use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::rate_limiter::RateLimiter;
use async_trait::async_trait;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct EndToEndProducingConsumerBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl EndToEndProducingConsumerBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for EndToEndProducingConsumerBenchmark {
    async fn run(&mut self) -> BenchmarkFutures {
        self.init_streams().await.expect("Failed to init streams!");
        let actors_count = self.args.producers();
        info!("Creating {} producing consumer(s)...", actors_count);
        let streams_number = self.args.streams();
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();
        let message_size = self.args.message_size();
        let partitions_count = self.args.number_of_partitions();
        let warmup_time = self.args.warmup_time();

        let mut futures: BenchmarkFutures = Ok(Vec::with_capacity(actors_count as usize));
        for actor_id in 1..=actors_count {
            let args = self.args.clone();
            let client_factory = self.client_factory.clone();
            info!(
                "Executing the benchmark on producing consumer #{}...",
                actor_id
            );
            let stream_id = args.start_stream_id() + 1 + (actor_id % streams_number);

            let actor = ProducingConsumer::new(
                client_factory,
                args.kind(),
                actor_id,
                stream_id,
                partitions_count,
                messages_per_batch,
                message_batches,
                message_size,
                warmup_time,
                args.sampling_time(),
                args.moving_average_window(),
                args.rate_limit()
                    .map(|rl| RateLimiter::new(rl.as_bytes_u64())),
                false,
            );
            let future = Box::pin(async move { actor.run().await });
            futures.as_mut().unwrap().push(future);
        }
        info!("Created {} producing consumer(s).", actors_count);
        futures
    }

    fn kind(&self) -> BenchmarkKind {
        self.args.kind()
    }

    fn total_messages(&self) -> u64 {
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();
        let streams = self.args.streams();
        (messages_per_batch * message_batches * streams) as u64
    }

    fn args(&self) -> &IggyBenchArgs {
        &self.args
    }

    fn client_factory(&self) -> &Arc<dyn ClientFactory> {
        &self.client_factory
    }
}
