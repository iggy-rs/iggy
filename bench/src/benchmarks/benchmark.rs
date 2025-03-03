use crate::args::kind::BenchmarkKindCommand;
use crate::{args::common::IggyBenchArgs, utils::client_factory::create_client_factory};
use async_trait::async_trait;
use futures::Future;
use iggy::client::{StreamClient, TopicClient};
use iggy::clients::client::IggyClient;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::error::IggyError;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use iggy_bench_report::individual_metrics::BenchmarkIndividualMetrics;
use integration::test_server::{login_root, ClientFactory};
use std::{pin::Pin, sync::Arc};
use tracing::info;

use super::balanced_consumer_group::BalancedConsumerGroupBenchmark;
use super::balanced_producer::BalancedProducerBenchmark;
use super::balanced_producer_and_consumer_group::BalancedProducerAndConsumerGroupBenchmark;
use super::end_to_end_producing_consumer::EndToEndProducingConsumerBenchmark;
use super::end_to_end_producing_consumer_group::EndToEndProducingConsumerGroupBenchmark;
use super::pinned_consumer::PinnedConsumerBenchmark;
use super::pinned_producer::PinnedProducerBenchmark;
use super::pinned_producer_and_consumer::PinnedProducerAndConsumerBenchmark;

pub type BenchmarkFutures = Result<
    Vec<Pin<Box<dyn Future<Output = Result<BenchmarkIndividualMetrics, IggyError>> + Send>>>,
    IggyError,
>;

impl From<IggyBenchArgs> for Box<dyn Benchmarkable> {
    fn from(args: IggyBenchArgs) -> Self {
        let client_factory = create_client_factory(&args);

        match args.benchmark_kind {
            BenchmarkKindCommand::PinnedProducer(_) => {
                PinnedProducerBenchmark::new(Arc::new(args), client_factory)
            }

            BenchmarkKindCommand::PinnedConsumer(_) => {
                PinnedConsumerBenchmark::new(Arc::new(args), client_factory)
            }

            BenchmarkKindCommand::PinnedProducerAndConsumer(_) => {
                PinnedProducerAndConsumerBenchmark::new(Arc::new(args), client_factory)
            }

            BenchmarkKindCommand::BalancedProducer(_) => {
                BalancedProducerBenchmark::new(Arc::new(args), client_factory)
            }

            BenchmarkKindCommand::BalancedConsumerGroup(_) => {
                BalancedConsumerGroupBenchmark::new(Arc::new(args), client_factory)
            }

            BenchmarkKindCommand::BalancedProducerAndConsumerGroup(_) => {
                BalancedProducerAndConsumerGroupBenchmark::new(Arc::new(args), client_factory)
            }

            BenchmarkKindCommand::EndToEndProducingConsumer(_) => {
                EndToEndProducingConsumerBenchmark::new(Arc::new(args), client_factory)
            }

            BenchmarkKindCommand::EndToEndProducingConsumerGroup(_) => {
                EndToEndProducingConsumerGroupBenchmark::new(Arc::new(args), client_factory)
            }

            BenchmarkKindCommand::Examples => unreachable!(),
        }
    }
}

#[async_trait]
pub trait Benchmarkable {
    async fn run(&self) -> BenchmarkFutures;
    fn kind(&self) -> BenchmarkKind;
    fn args(&self) -> &IggyBenchArgs;
    fn client_factory(&self) -> &Arc<dyn ClientFactory>;
    fn print_info(&self);

    /// Below methods have common implementation for all benchmarks.
    /// Initializes the streams and topics for the benchmark.
    /// This method is called before the benchmark is executed.
    async fn init_streams(&self) -> Result<(), IggyError> {
        let start_stream_id = self.args().start_stream_id();
        let number_of_streams = self.args().streams();
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
                let name = format!("topic {}", topic_id);
                let max_topic_size = match self.args().max_topic_size() {
                    Some(size) => MaxTopicSize::Custom(size),
                    None => MaxTopicSize::Unlimited,
                };

                info!(
                    "Creating the test topic {} for stream {} with max topic size: {:?}",
                    topic_id, stream_id, max_topic_size
                );

                client
                    .create_topic(
                        &stream_id.try_into()?,
                        &name,
                        partitions_count,
                        CompressionAlgorithm::default(),
                        None,
                        None,
                        IggyExpiry::NeverExpire,
                        max_topic_size,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn check_streams(&self) -> Result<(), IggyError> {
        let start_stream_id = self.args().start_stream_id();
        let number_of_streams = self.args().streams();
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

    fn common_params_str(&self) -> String {
        let message_size = format!("message size: {} b,", self.args().message_size());
        let messages_per_batch = format!(
            " messages per batch: {} b,",
            self.args().messages_per_batch()
        );
        let data = if let Some(data) = self.args().total_data() {
            format!(" total data to send: {},", data)
        } else {
            format!(
                " total batches to send: {},",
                self.args().message_batches().unwrap()
            )
        };
        let rate_limit = self
            .args()
            .rate_limit()
            .map(|rl| format!(" global rate limit: {rl}/s"))
            .unwrap_or_default();

        format!(
            "{}{}{}{}",
            message_size, messages_per_batch, data, rate_limit,
        )
    }
}
