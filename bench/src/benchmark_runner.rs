use crate::args::common::IggyBenchArgs;
use crate::benchmark_result::BenchmarkResults;
use crate::benchmarks::benchmark::Benchmarkable;
use crate::server_starter::start_server_if_needed;
use futures::future::select_all;
use iggy::error::IggyError;
use integration::test_server::TestServer;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

pub struct BenchmarkRunner {
    args: Option<IggyBenchArgs>,
    test_server: Option<TestServer>,
}

impl BenchmarkRunner {
    pub fn new(args: IggyBenchArgs) -> Self {
        Self {
            args: Some(args),
            test_server: None,
        }
    }

    pub async fn run(&mut self) -> Result<(), IggyError> {
        let mut args = self.args.take().unwrap();
        self.test_server = start_server_if_needed(&mut args).await;

        let transport = args.transport();
        let server_addr = args.server_address();
        info!("Starting to benchmark: {transport} with server: {server_addr}",);

        let mut benchmark: Box<dyn Benchmarkable> = args.into();
        let mut join_handles = benchmark.run().await?;

        let mut results = Vec::new();

        while !join_handles.is_empty() {
            let (result, _index, remaining) = select_all(join_handles).await;
            join_handles = remaining;

            match result {
                Ok(r) => results.push(r),
                Err(e) => return Err(e),
            }
        }

        // Sleep just to see result prints after all the join handles are done and tcp connections are closed
        sleep(Duration::from_millis(10)).await;
        let results: BenchmarkResults = results.into();
        benchmark.display_settings();
        results
            .to_string()
            .split('\n')
            .for_each(|result| info!("{}", result));
        Ok(())
    }
}
