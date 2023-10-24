use super::{
    poll_benchmark::PollMessagesBenchmark, send_and_poll_benchmark::SendAndPollMessagesBenchmark,
    send_benchmark::SendMessagesBenchmark,
};
use crate::{
    args::{common::IggyBenchArgs, simple::BenchmarkKind},
    benchmark_result::BenchmarkResult,
    client_factory::create_client_factory,
};
use async_trait::async_trait;
use futures::Future;
use iggy::{
    client::{StreamClient, TopicClient},
    clients::client::{IggyClient, IggyClientConfig},
    error::Error,
    identifier::Identifier,
    streams::{create_stream::CreateStream, get_streams::GetStreams},
    topics::create_topic::CreateTopic,
};
use integration::test_server::{login_root, ClientFactory};
use std::{pin::Pin, sync::Arc};
use tracing::info;

pub type BenchmarkFutures =
    Result<Vec<Pin<Box<dyn Future<Output = Result<BenchmarkResult, Error>> + Send>>>, Error>;

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
    async fn init_streams(&self) -> Result<(), Error> {
        let start_stream_id = self.args().start_stream_id();
        let number_of_streams = self.args().number_of_streams();
        let topic_id: u32 = 1;
        let partitions_count: u32 = 1;
        let client = self.client_factory().create_client().await;
        let client = IggyClient::create(client, IggyClientConfig::default(), None, None, None);
        login_root(&client).await;
        let streams = client.get_streams(&GetStreams {}).await?;
        for i in 1..=number_of_streams {
            let stream_id = start_stream_id + i;
            if streams.iter().all(|s| s.id != stream_id) {
                info!("Creating the test stream {}", stream_id);
                let name = format!("stream {}", stream_id);
                client
                    .create_stream(&CreateStream { stream_id, name })
                    .await?;

                info!(
                    "Creating the test topic {} for stream {}",
                    topic_id, stream_id
                );
                let name = format!("topic {}", topic_id);
                client
                    .create_topic(&CreateTopic {
                        stream_id: Identifier::numeric(stream_id)?,
                        topic_id,
                        partitions_count,
                        name,
                        message_expiry: None,
                    })
                    .await?;
            }
        }
        Ok(())
    }

    async fn check_streams(&self) -> Result<(), Error> {
        let start_stream_id = self.args().start_stream_id();
        let number_of_streams = self.args().number_of_streams();
        let client = self.client_factory().create_client().await;
        let client = IggyClient::create(client, IggyClientConfig::default(), None, None, None);
        login_root(&client).await;
        let streams = client.get_streams(&GetStreams {}).await?;
        for i in 1..=number_of_streams {
            let stream_id = start_stream_id + i;
            if streams.iter().all(|s| s.id != stream_id) {
                return Err(Error::ResourceNotFound(format!(
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
