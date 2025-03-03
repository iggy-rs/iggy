use crate::analytics::report_builder::BenchmarkReportBuilder;
use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::Benchmarkable;
use crate::plot::{plot_chart, ChartType};
use crate::utils::cpu_name::append_cpu_name_lowercase;
use crate::utils::server_starter::start_server_if_needed;
use crate::utils::{collect_server_logs_and_save_to_file, params_from_args_and_metrics};
use futures::future::select_all;
use iggy::error::IggyError;
use iggy_bench_report::hardware::BenchmarkHardware;
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
        let should_open_charts = args.open_charts();
        self.test_server = start_server_if_needed(&mut args).await;

        let transport = args.transport();
        let server_addr = args.server_address();
        info!("Starting to benchmark: {transport} with server: {server_addr}",);

        let benchmark: Box<dyn Benchmarkable> = args.into();
        benchmark.print_info();
        let mut join_handles = benchmark.run().await?;

        let mut individual_metrics = Vec::new();

        while !join_handles.is_empty() {
            let (result, _index, remaining) = select_all(join_handles).await;
            join_handles = remaining;

            match result {
                Ok(r) => individual_metrics.push(r),
                Err(e) => return Err(e),
            }
        }

        info!("All actors joined!");

        let hardware =
            BenchmarkHardware::get_system_info_with_identifier(benchmark.args().identifier());
        let params = params_from_args_and_metrics(benchmark.args(), &individual_metrics);
        let transport = params.transport;
        let server_addr = params.server_address.clone();

        let report = BenchmarkReportBuilder::build(
            hardware,
            params,
            individual_metrics,
            benchmark.args().moving_average_window(),
        )
        .await;

        // Sleep just to see result prints after all tasks are joined (they print per-actor results)
        sleep(Duration::from_millis(10)).await;

        report.print_summary();

        if let Some(output_dir) = benchmark.args().output_dir() {
            // Generate the full output path using the directory name generator
            let mut dir_name = benchmark.args().generate_dir_name();
            append_cpu_name_lowercase(&mut dir_name);
            let full_output_path = Path::new(&output_dir)
                .join(dir_name.clone())
                .to_string_lossy()
                .to_string();

            // Dump the report to JSON
            report.dump_to_json(&full_output_path);

            if let Err(e) = collect_server_logs_and_save_to_file(
                &transport,
                &server_addr,
                Path::new(&full_output_path),
            )
            .await
            {
                error!("Failed to collect server logs: {e}");
            }

            // Generate the plots
            plot_chart(
                &report,
                &full_output_path,
                ChartType::Throughput,
                should_open_charts,
            )
            .map_err(|e| {
                error!("Failed to generate plots: {e}");
                IggyError::CannotWriteToFile
            })?;
            plot_chart(
                &report,
                &full_output_path,
                ChartType::Latency,
                should_open_charts,
            )
            .map_err(|e| {
                error!("Failed to generate plots: {e}");
                IggyError::CannotWriteToFile
            })?;
        }

        Ok(())
    }
}
