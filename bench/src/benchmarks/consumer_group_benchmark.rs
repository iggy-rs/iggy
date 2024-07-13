use crate::{
    args::{common::IggyBenchArgs, simple::BenchmarkKind},
    consumer::Consumer,
};
use async_trait::async_trait;
use iggy::{
    client::ConsumerGroupClient,
    clients::client::{IggyClient, IggyClientBackgroundConfig},
    error::IggyError,
    utils::byte_size::IggyByteSize,
};
use integration::test_server::{login_root, ClientFactory};
use std::sync::Arc;
use tracing::info;

use super::benchmark::{BenchmarkFutures, Benchmarkable};

pub struct ConsumerGroupBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl ConsumerGroupBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }

    pub async fn init_consumer_group(
        &self,
        consumer_group_name: &str,
        consumer_group_id: u32,
    ) -> Result<(), IggyError> {
        let start_stream_id = self.args().start_stream_id();
        let stream_id = start_stream_id + 1;
        let topic_id: u32 = 1;
        let client = self.client_factory().create_client().await;
        let client = IggyClient::create(
            client,
            IggyClientBackgroundConfig::default(),
            None,
            None,
            None,
        );
        login_root(&client).await;

        info!(
            "Creating test consumer group with name: {}, id: {}",
            consumer_group_name, consumer_group_id
        );
        client
            .create_consumer_group(
                &stream_id.try_into().unwrap(),
                &topic_id.try_into().unwrap(),
                consumer_group_name,
                Some(consumer_group_id),
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Benchmarkable for ConsumerGroupBenchmark {
    async fn run(&mut self) -> BenchmarkFutures {
        self.init_streams().await.expect("Failed to init streams!");
        let consumer_group_id = 1;
        let consumer_group_name = "cg-1";
        self.init_consumer_group(consumer_group_name, consumer_group_id)
            .await
            .expect("Failed to init consumer group");
        let start_stream_id = self.args.start_stream_id();
        let consumers = self.args.consumers();
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();
        let warmup_time = self.args.warmup_time();
        let mut futures: BenchmarkFutures = Ok(Vec::with_capacity((consumers) as usize));

        for consumer_id in 1..=consumers {
            let stream_id = start_stream_id + 1;
            let consumer = Consumer::new(
                self.client_factory.clone(),
                consumer_id,
                Some(consumer_group_id),
                stream_id,
                messages_per_batch,
                message_batches,
                warmup_time,
            );
            let future = Box::pin(async move { consumer.run().await });
            futures.as_mut().unwrap().push(future);
        }
        info!(
            "Starting consumer group benchmark {} messages",
            self.total_messages()
        );
        futures
    }

    fn kind(&self) -> BenchmarkKind {
        BenchmarkKind::ConsumerGroupPoll
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
        // TODO(numinex) - add more details about consumer groups.
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
