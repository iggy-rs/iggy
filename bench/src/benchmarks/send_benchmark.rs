use super::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::args::common::IggyBenchArgs;
use crate::args::simple::BenchmarkKind;
use crate::producer::Producer;
use async_trait::async_trait;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct SendMessagesBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl SendMessagesBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for SendMessagesBenchmark {
    async fn run(&mut self) -> BenchmarkFutures {
        self.init_streams().await.expect("Failed to init streams!");
        let clients_count = self.args.producers();
        info!("Creating {} client(s)...", clients_count);
        let streams_number = self.args.number_of_streams();
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();
        let message_size = self.args.message_size();
        let partitions_count = self.args.number_of_partitions();
        let warmup_time = self.args.warmup_time();

        let mut futures: BenchmarkFutures = Ok(Vec::with_capacity(clients_count as usize));
        for client_id in 1..=clients_count {
            let args = self.args.clone();
            let client_factory = self.client_factory.clone();
            info!("Executing the benchmark on client #{}...", client_id);
            let args = args.clone();
            let start_stream_id = args.start_stream_id();
            let client_factory = client_factory.clone();

            let parallel_producer_streams = !args.disable_parallel_producer_streams();
            let stream_id = match parallel_producer_streams {
                true => start_stream_id + client_id,
                false => start_stream_id + 1 + (client_id % streams_number),
            };

            let producer = Producer::new(
                client_factory,
                client_id,
                stream_id,
                partitions_count,
                messages_per_batch,
                message_batches,
                message_size,
                warmup_time,
            );
            let future = Box::pin(async move { producer.run().await });
            futures.as_mut().unwrap().push(future);
        }
        info!("Created {} client(s).", clients_count);
        futures
    }

    fn kind(&self) -> BenchmarkKind {
        BenchmarkKind::Send
    }

    fn total_messages(&self) -> u64 {
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();
        let streams = self.args.number_of_streams();
        (messages_per_batch * message_batches * streams) as u64
    }

    fn args(&self) -> &IggyBenchArgs {
        &self.args
    }

    fn client_factory(&self) -> &Arc<dyn ClientFactory> {
        &self.client_factory
    }

    fn display_settings(&self) {
        let total_messages = self.total_messages();
        let total_size_bytes = total_messages * self.args().message_size() as u64;
        info!(
                "\x1B[32mBenchmark: {}, total messages: {}, total size: {} bytes, {} streams, {} messages per batch, {} batches, {} bytes per message, {} producers\x1B[0m",
                self.kind(),
                total_messages,
                total_size_bytes,
                self.args().number_of_streams(),
                self.args().messages_per_batch(),
                self.args().message_batches(),
                self.args().message_size(),
                self.args().producers(),
            );
    }
}
