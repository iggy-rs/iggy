use crate::analytics::report_builder::BenchmarkReportBuilder;
use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::Benchmarkable;
use crate::plot::{plot_chart, ChartType};
use crate::utils::server_starter::start_server_if_needed;
use crate::utils::server_version::get_server_version;
use futures::future::select_all;
use iggy::error::IggyError;
use iggy_benchmark_report::hardware::BenchmarkHardware;
use iggy_benchmark_report::params::BenchmarkParams;
use integration::test_server::TestServer;
use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

pub struct BenchmarkRunner {
    pub args: Option<IggyBenchArgs>,
    pub test_server: Option<TestServer>,
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
        info!("Benchmarking finished");

        let mut individual_metrics = Vec::new();

        while !join_handles.is_empty() {
            let (result, _index, remaining) = select_all(join_handles).await;
            join_handles = remaining;

            match result {
                Ok(r) => individual_metrics.push(r),
                Err(e) => return Err(e),
            }
        }

        info!("All actors finished");

        let params = BenchmarkParams::from(benchmark.args());

        let server_version = match get_server_version(&params).await {
            Ok(v) => v,
            Err(_) => "unknown".to_string(),
        };
        let hardware =
            BenchmarkHardware::get_system_info_with_identifier(benchmark.args().identifier());
        let report = BenchmarkReportBuilder::build(
            server_version,
            hardware,
            params,
            individual_metrics,
            benchmark.args().moving_average_window(),
        );

        info!("Printing summary");

        // Sleep just to see result prints after all tasks are joined (they print per-actor results)
        sleep(Duration::from_millis(10)).await;

        report.print_summary();

        if let Some(output_dir) = benchmark.args().output_dir() {
            // Generate the full output path using the directory name generator
            let dir_name = benchmark.args().generate_dir_name();
            let full_output_path = Path::new(&output_dir)
                .join(dir_name)
                .to_string_lossy()
                .to_string();

            // Dump the report to JSON
            report.dump_to_json(&full_output_path);

            // Generate the plots
            plot_chart(&report, &full_output_path, ChartType::Throughput).map_err(|e| {
                error!("Failed to generate plots: {e}");
                IggyError::CannotWriteToFile
            })?;
            plot_chart(&report, &full_output_path, ChartType::Latency).map_err(|e| {
                error!("Failed to generate plots: {e}");
                IggyError::CannotWriteToFile
            })?;
        }

        Ok(())
    }
}
