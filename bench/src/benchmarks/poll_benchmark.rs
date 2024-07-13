use super::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::args::common::IggyBenchArgs;
use crate::args::simple::BenchmarkKind;
use crate::consumer::Consumer;
use async_trait::async_trait;
use iggy::utils::byte_size::IggyByteSize;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct PollMessagesBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl PollMessagesBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for PollMessagesBenchmark {
    async fn run(&mut self) -> BenchmarkFutures {
        self.check_streams().await?;
        let clients_count = self.args.consumers();
        info!("Creating {} client(s)...", clients_count);
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();

        let mut futures: BenchmarkFutures = Ok(Vec::with_capacity(clients_count as usize));
        for client_id in 1..=clients_count {
            let args = self.args.clone();
            let client_factory = self.client_factory.clone();
            info!("Executing the benchmark on client #{}...", client_id);
            let args = args.clone();
            let start_stream_id = args.start_stream_id();
            let client_factory = client_factory.clone();
            let parallel_consumer_streams = !args.disable_parallel_consumer_streams();
            let stream_id = match parallel_consumer_streams {
                true => start_stream_id + client_id,
                false => start_stream_id + 1,
            };
            let warmup_time = args.warmup_time();

            let consumer = Consumer::new(
                client_factory,
                client_id,
                None,
                stream_id,
                messages_per_batch,
                message_batches,
                warmup_time,
            );

            let future = Box::pin(async move { consumer.run().await });
            futures.as_mut().unwrap().push(future);
        }
        info!("Created {} client(s).", clients_count);
        futures
    }

    fn kind(&self) -> BenchmarkKind {
        BenchmarkKind::Poll
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
                "\x1B[32mBenchmark: {}, total messages: {}, processed: {}, {} streams, {} messages per batch, {} batches, {} bytes per message, {} consumers\x1B[0m",
                self.kind(),
                total_messages,
                IggyByteSize::from(total_size_bytes),
                self.args().number_of_streams(),
                self.args().messages_per_batch(),
                self.args().message_batches(),
                self.args().message_size(),
                self.args().consumers(),
            );
    }
}
