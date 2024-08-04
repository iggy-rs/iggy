use super::{
    consumer_group_benchmark::ConsumerGroupBenchmark, poll_benchmark::PollMessagesBenchmark,
    send_and_poll_benchmark::SendAndPollMessagesBenchmark, send_benchmark::SendMessagesBenchmark,
};
use crate::{
    args::{common::IggyBenchArgs, simple::BenchmarkKind},
    benchmark_result::BenchmarkResult,
    client_factory::create_client_factory,
};
use async_trait::async_trait;
use futures::Future;
use iggy::client::{StreamClient, TopicClient};
use iggy::clients::client::IggyClient;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::error::IggyError;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use integration::test_server::{login_root, ClientFactory};
use std::{pin::Pin, sync::Arc};
use tracing::info;

pub type BenchmarkFutures = Result<
    Vec<Pin<Box<dyn Future<Output = Result<BenchmarkResult, IggyError>> + Send>>>,
    IggyError,
>;

impl From<IggyBenchArgs> for Box<dyn Benchmarkable> {
    fn from(args: IggyBenchArgs) -> Self {
        let client_factory = create_client_factory(&args);
        let benchmark_kind = args.benchmark_kind.as_simple_kind();
        match benchmark_kind {
            BenchmarkKind::Poll => {
                Box::new(PollMessagesBenchmark::new(Arc::new(args), client_factory))
            }
            BenchmarkKind::Send => {
                Box::new(SendMessagesBenchmark::new(Arc::new(args), client_factory))
            }
            BenchmarkKind::ConsumerGroupPoll => {
                Box::new(ConsumerGroupBenchmark::new(Arc::new(args), client_factory))
            }
            BenchmarkKind::SendAndPoll => Box::new(SendAndPollMessagesBenchmark::new(
                Arc::new(args),
                client_factory,
            )),
        }
    }
}

#[async_trait]
pub trait Benchmarkable {
    async fn run(&mut self) -> BenchmarkFutures;
    fn kind(&self) -> BenchmarkKind;
    fn args(&self) -> &IggyBenchArgs;
    fn client_factory(&self) -> &Arc<dyn ClientFactory>;
    fn display_settings(&self);

    /// Below methods have common implementation for all benchmarks.

    /// Initializes the streams and topics for the benchmark.
    /// This method is called before the benchmark is executed.
    async fn init_streams(&self) -> Result<(), IggyError> {
        let start_stream_id = self.args().start_stream_id();
        let number_of_streams = self.args().number_of_streams();
        let topic_id: u32 = 1;
        let partitions_count: u32 = self.args().number_of_partitions();
        let client = self.client_factory().create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;
        let streams = client.get_streams().await?;
        for i in 1..=number_of_streams {
            let stream_id = start_stream_id + i;
            if streams.iter().all(|s| s.id != stream_id) {
                info!("Creating the test stream {}", stream_id);
                let name = format!("stream {}", stream_id);
                client.create_stream(&name, Some(stream_id)).await?;

                info!(
                    "Creating the test topic {} for stream {}",
                    topic_id, stream_id
                );
                let name = format!("topic {}", topic_id);
                client
                    .create_topic(
                        &stream_id.try_into()?,
                        &name,
                        partitions_count,
                        CompressionAlgorithm::default(),
                        None,
                        None,
                        IggyExpiry::NeverExpire,
                        MaxTopicSize::Unlimited,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn check_streams(&self) -> Result<(), IggyError> {
        let start_stream_id = self.args().start_stream_id();
        let number_of_streams = self.args().number_of_streams();
        let client = self.client_factory().create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;
        let streams = client.get_streams().await?;
        for i in 1..=number_of_streams {
            let stream_id = start_stream_id + i;
            if streams.iter().all(|s| s.id != stream_id) {
                return Err(IggyError::ResourceNotFound(format!(
                    "Streams for testing are not properly initialized. Stream with id: {} is missing.",
                    stream_id
                )));
            }
        }
        Ok(())
    }

    /// Returns the total number of messages that will be sent or polled by the benchmark.
    fn total_messages(&self) -> u64 {
        let messages_per_batch = self.args().messages_per_batch();
        let message_batches = self.args().message_batches();
        let streams = self.args().number_of_streams();
        (messages_per_batch * message_batches * streams) as u64
    }
}
