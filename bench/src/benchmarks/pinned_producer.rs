use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::benchmarks::common::*;
use async_trait::async_trait;
use iggy::utils::topic_size::MaxTopicSize;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct PinnedProducerBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl PinnedProducerBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Box<Self> {
        Box::new(Self {
            args,
            client_factory,
        })
    }
}

#[async_trait]
impl Benchmarkable for PinnedProducerBenchmark {
    async fn run(&self) -> BenchmarkFutures {
        self.init_streams().await?;
        let client_factory = &self.client_factory;
        let args = self.args.clone();

        build_producer_futures(client_factory, &args)
    }

    fn kind(&self) -> BenchmarkKind {
        self.args.kind()
    }

    fn args(&self) -> &IggyBenchArgs {
        &self.args
    }

    fn client_factory(&self) -> &Arc<dyn ClientFactory> {
        &self.client_factory
    }

    fn print_info(&self) {
        let streams = format!("streams: {}", self.args.streams());
        let producers = format!("producers: {}", self.args.producers());
        let max_topic_size = match self.args.max_topic_size() {
            Some(size) => format!(" max topic size: {}", size),
            None => format!(" max topic size: {}", MaxTopicSize::ServerDefault),
        };
        let common_params = self.common_params_str();

        info!("Staring benchmark PinnedProducer, {streams}, {producers}, {max_topic_size}, {common_params}");
    }
}
