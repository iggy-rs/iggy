use crate::{
    args::{common::IggyBenchArgs, simple::BenchmarkKind},
    benchmarks::{CONSUMER_GROUP_BASE_ID, CONSUMER_GROUP_NAME_PREFIX},
    consumer::Consumer,
};
use async_trait::async_trait;
use iggy::{
    client::ConsumerGroupClient, clients::client::IggyClient, error::IggyError,
    utils::byte_size::IggyByteSize,
};
use integration::test_server::{login_root, ClientFactory};
use std::sync::Arc;
use tracing::{error, info};

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

    pub async fn init_consumer_groups(&self, consumer_groups_count: u32) -> Result<(), IggyError> {
        let start_stream_id = self.args().start_stream_id();
        let topic_id: u32 = 1;
        let client = self.client_factory().create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;
        for i in 1..=consumer_groups_count {
            let consumer_group_id = CONSUMER_GROUP_BASE_ID + i;
            let stream_id = start_stream_id + i;
            let consumer_group_name =
                format!("{}-{}", CONSUMER_GROUP_NAME_PREFIX, consumer_group_id);
            info!(
                "Creating test consumer group with name: {}, id: {}, stream id: {}, topic id: {}",
                consumer_group_name, consumer_group_id, stream_id, topic_id
            );

            let cg = client
                .create_consumer_group(
                    &stream_id.try_into().unwrap(),
                    &topic_id.try_into().unwrap(),
                    &consumer_group_name,
                    Some(consumer_group_id),
                )
                .await;
            if cg.is_err() {
                let error = cg.err().unwrap();
                match error {
                    IggyError::ConsumerGroupIdAlreadyExists(_, _) => {
                        continue;
                    }
                    _ => error!("Error when creating consumer group : {error}"),
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Benchmarkable for ConsumerGroupBenchmark {
    async fn run(&mut self) -> BenchmarkFutures {
        self.check_streams().await?;
        let consumer_groups_count = self.args.number_of_consumer_groups();
        self.init_consumer_groups(consumer_groups_count)
            .await
            .expect("Failed to init consumer group");

        let start_stream_id = self.args.start_stream_id();
        let start_consumer_group_id = CONSUMER_GROUP_BASE_ID;
        let consumers = self.args.consumers();
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();
        let warmup_time = self.args.warmup_time();
        let mut futures: BenchmarkFutures = Ok(Vec::with_capacity((consumers) as usize));

        for consumer_id in 1..=consumers {
            let consumer_group_id =
                start_consumer_group_id + 1 + (consumer_id % consumer_groups_count);
            let stream_id = start_stream_id + 1 + (consumer_id % consumer_groups_count);
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
            "Starting consumer group benchmark with {} messages",
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
