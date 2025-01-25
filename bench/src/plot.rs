use charming::theme::Theme;
use charming::{Chart, HtmlRenderer};
use iggy::utils::byte_size::IggyByteSize;
use iggy_benchmark_report::report::BenchmarkReport;
use std::path::Path;
use std::time::Instant;
use tracing::info;

pub enum ChartType {
    Throughput,
    Latency,
}

impl ChartType {
    fn name(&self) -> &'static str {
        match self {
            ChartType::Throughput => "throughput",
            ChartType::Latency => "latency",
        }
    }

    fn create_chart(&self) -> fn(&BenchmarkReport, bool) -> Chart {
        match self {
            ChartType::Throughput => iggy_benchmark_report::create_throughput_chart,
            ChartType::Latency => iggy_benchmark_report::create_latency_chart,
        }
    }

    fn get_samples(&self, report: &BenchmarkReport) -> usize {
        match self {
            ChartType::Throughput => report
                .individual_metrics
                .iter()
                .map(|m| m.throughput_mb_ts.points.len())
                .sum(),
            ChartType::Latency => report
                .individual_metrics
                .iter()
                .filter(|m| !m.latency_ts.points.is_empty())
                .map(|m| m.latency_ts.points.len())
                .sum(),
        }
    }
}

pub fn plot_chart(
    report: &BenchmarkReport,
    output_directory: &str,
    chart_type: ChartType,
) -> std::io::Result<()> {
    let data_processing_start = Instant::now();
    let chart = (chart_type.create_chart())(report, true); // Use dark theme by default
    let data_processing_time = data_processing_start.elapsed();

    let chart_render_start = Instant::now();
    let chart_path = format!("{}/{}.html", output_directory, chart_type.name());
    save_chart(&chart, chart_type.name(), output_directory, 1600, 1200)?;
    let chart_render_time = chart_render_start.elapsed();

    let total_samples = chart_type.get_samples(report);
    let report_path = format!("{}/report.json", output_directory);
    let report_size = IggyByteSize::from(std::fs::metadata(&report_path)?.len());

    info!(
        "Generated {} plot at: {} ({} samples, report.json size: {}, data processing: {:.2?}, chart render: {:.2?})",
        chart_type.name(),
        chart_path,
        total_samples,
        report_size,
        data_processing_time,
        chart_render_time
    );
    Ok(())
}

fn save_chart(
    chart: &Chart,
    file_name: &str,
    output_directory: &str,
    width: u64,
    height: u64,
) -> std::io::Result<()> {
    let parent = Path::new(output_directory).parent().unwrap();
    std::fs::create_dir_all(parent)?;
    let full_output_path = Path::new(output_directory).join(format!("{}.html", file_name));

    let mut renderer = HtmlRenderer::new(file_name, width, height).theme(Theme::Dark);
    renderer.save(chart, &full_output_path).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to save HTML plot: {}", e),
        )
    })
}
