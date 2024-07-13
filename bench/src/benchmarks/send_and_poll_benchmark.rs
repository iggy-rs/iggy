use super::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::args::common::IggyBenchArgs;
use crate::args::simple::BenchmarkKind;
use crate::consumer::Consumer;
use crate::producer::Producer;
use async_trait::async_trait;
use colored::Colorize;
use human_format::Formatter;
use iggy::utils::byte_size::IggyByteSize;
use integration::test_server::ClientFactory;
use std::fmt::Display;
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

impl Display for SendAndPollMessagesBenchmark {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let total_messages = self.total_messages();
        let processed = IggyByteSize::from(total_messages * self.args().message_size() as u64);
        let total_messages_human_readable = Formatter::new().format(total_messages as f64);
        let info = format!("Benchmark: {}, transport: {}, total messages: {}, processed: {}, {} streams, {} messages per batch, {} batches, {} bytes per message, {} producers, {} consumers",
                self.kind(),
                self.args().transport(),
                total_messages_human_readable,
                processed,
                self.args().number_of_streams(),
                self.args().messages_per_batch(),
                self.args().message_batches(),
                self.args().message_size(),
                self.args().producers(),
                self.args().consumers(),
            ).green();

        writeln!(f, "{}", info)
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

    fn display_settings(&self) {
        info!("{}", self.to_string());
    }
}
