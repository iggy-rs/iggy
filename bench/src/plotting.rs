use crate::statistics::record::BenchmarkRecord;
use crate::{
    benchmark_result::BenchmarkOutput, statistics::actor_statistics::BenchmarkActorStatistics,
};
use charming::{
    component::{Axis, Grid, Title},
    element::{
        AxisLabel, AxisPointer, AxisPointerType, LineStyle, NameLocation, Symbol, Tooltip, Trigger,
    },
    series::Line,
    Chart, HtmlRenderer,
};
use std::path::Path;
use tracing::info;

fn format_throughput_stats(stats: &BenchmarkActorStatistics, title_prefix: &str) -> String {
    format!(
        "Average throughput per {}: {:.2} msg/s",
        if title_prefix == "Producer" {
            "producer"
        } else {
            "consumer"
        },
        stats.throughput_messages_per_second,
    )
}

fn format_latency_stats(stats: &BenchmarkActorStatistics) -> String {
    format!(
        "Average: {:.2} ms, Median: {:.2} ms, P99: {:.2} ms, P999: {:.2} ms",
        stats.avg_latency_ms, stats.median_latency_ms, stats.p99_latency_ms, stats.p999_latency_ms,
    )
}

fn format_throughput_mb_stats(stats: &BenchmarkActorStatistics, title_prefix: &str) -> String {
    format!(
        "Average throughput per {}: {:.2} MB/s",
        if title_prefix == "Producer" {
            "producer"
        } else {
            "consumer"
        },
        stats.throughput_megabytes_per_second,
    )
}

pub fn generate_plots(output: &BenchmarkOutput, output_dir: &str) -> std::io::Result<()> {
    let actors_info = match (output.params.producers, output.params.consumers) {
        (0, c) => format!("{} consumers", c),
        (p, 0) => format!("{} producers", p),
        (p, c) => format!("{} producers/{} consumers", p, c),
    };

    let mut subtext = format!(
        "{}, {} msg/batch, {} batches, {} bytes/msg",
        actors_info,
        output.params.messages_per_batch,
        output.params.message_batches,
        output.params.message_size
    );

    if let Some(overall_stats) = &output.overall_statistics {
        subtext = format!(
            "{}\nTotal throughput: {:.2} MB/s, {:.0} messages/s",
            subtext,
            overall_stats.total_throughput_megabytes_per_second,
            overall_stats.total_throughput_messages_per_second
        );
    }

    if let Some(producer_data) = &output.first_producer_raw_data {
        if let Some(producer_stats) = &output.first_producer_statistics {
            plot_throughput_over_time(
                producer_data,
                producer_stats,
                "Producer",
                &Path::new(output_dir)
                    .join("producer_throughput")
                    .to_string_lossy(),
                &subtext,
            )?;
            plot_throughput_mb_over_time(
                producer_data,
                producer_stats,
                "Producer",
                &Path::new(output_dir)
                    .join("producer_throughput_mb")
                    .to_string_lossy(),
                &subtext,
            )?;
            plot_latency_over_time(
                producer_data,
                producer_stats,
                "Producer",
                &Path::new(output_dir)
                    .join("producer_latency")
                    .to_string_lossy(),
                &subtext,
            )?;
        }
    }

    if let Some(consumer_data) = &output.first_consumer_raw_data {
        if let Some(consumer_stats) = &output.first_consumer_statistics {
            plot_throughput_over_time(
                consumer_data,
                consumer_stats,
                "Consumer",
                &Path::new(output_dir)
                    .join("consumer_throughput")
                    .to_string_lossy(),
                &subtext,
            )?;
            plot_throughput_mb_over_time(
                consumer_data,
                consumer_stats,
                "Consumer",
                &Path::new(output_dir)
                    .join("consumer_throughput_mb")
                    .to_string_lossy(),
                &subtext,
            )?;
            plot_latency_over_time(
                consumer_data,
                consumer_stats,
                "Consumer",
                &Path::new(output_dir)
                    .join("consumer_latency")
                    .to_string_lossy(),
                &subtext,
            )?;
        }
    }

    Ok(())
}

fn calculate_moving_average(data: &[Vec<f64>], window_size: usize) -> Vec<Vec<f64>> {
    if data.is_empty() || window_size == 0 {
        return vec![];
    }

    let mut result = Vec::with_capacity(data.len());
    for i in 0..data.len() {
        let start = if i < window_size / 2 {
            0
        } else {
            i - window_size / 2
        };
        let end = std::cmp::min(i + window_size / 2 + 1, data.len());
        let window = &data[start..end];

        let sum: f64 = window.iter().map(|point| point[1]).sum();
        let avg = sum / window.len() as f64;

        result.push(vec![data[i][0], avg]);
    }
    result
}

fn plot_throughput_over_time(
    data: &[BenchmarkRecord],
    stats: &BenchmarkActorStatistics,
    title_prefix: &str,
    output_path: &str,
    subtext: &str,
) -> std::io::Result<()> {
    // Calculate throughput per second
    let points: Vec<_> = data
        .windows(2)
        .map(|w| {
            let time_diff = (w[1].elapsed_time_us - w[0].elapsed_time_us) as f64 / 1_000_000.0;
            let messages_diff = (w[1].messages - w[0].messages) as f64;
            let throughput = messages_diff / time_diff;
            let time_point = w[1].elapsed_time_us as f64 / 1_000_000.0;
            vec![time_point, throughput]
        })
        .collect();

    // Calculate moving average with a window of 20 points
    let smoothed_points = calculate_moving_average(&points, 20);

    let title = format!("{} Throughput Over Time", title_prefix);
    let stats_text = format_throughput_stats(stats, title_prefix);
    let full_subtext = format!("{}\n{}", subtext, stats_text);

    let chart = Chart::new()
        .tooltip(Tooltip::new().trigger(Trigger::Axis))
        .axis_pointer(AxisPointer::new().type_(AxisPointerType::Cross))
        .title(
            Title::new()
                .text(title.clone())
                .subtext(full_subtext)
                .padding(5)
                .item_gap(5)
                .left("center")
                .top(5),
        )
        .grid(
            Grid::new()
                .left("10%")
                .right("15%")
                .top("10%")
                .bottom("10%"),
        )
        .x_axis(
            Axis::new()
                .name("Time (seconds)")
                .name_location(NameLocation::Center)
                .name_gap(35)
                .axis_label(AxisLabel::new().formatter("{value} s")),
        )
        .y_axis(
            Axis::new()
                .name("Messages per Second")
                .name_location(NameLocation::Middle)
                .name_gap(50)
                .position("left")
                .axis_label(AxisLabel::new().formatter("{value} msg/s")),
        )
        .series(
            Line::new()
                .name("Raw Throughput")
                .symbol(Symbol::None)
                .data(points)
                .line_style(LineStyle::new().opacity(0.3)), // Make raw data more transparent
        )
        .series(
            Line::new()
                .name("Moving Average (20 points)")
                .symbol(Symbol::None)
                .data(smoothed_points)
                .line_style(LineStyle::new().width(2.5)), // Make smoothed line thicker
        );

    // Save as HTML
    let mut renderer = HtmlRenderer::new(title, 1600, 1200);
    let html_path = format!("{}.html", output_path);
    info!("Generating throughput HTML plot: {}", html_path);

    // Create parent directories if they don't exist
    if let Some(parent) = Path::new(&html_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    renderer.save(&chart, &html_path).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to save HTML plot: {}", e),
        )
    })?;

    Ok(())
}

fn plot_latency_over_time(
    data: &[BenchmarkRecord],
    stats: &BenchmarkActorStatistics,
    title_prefix: &str,
    output_path: &str,
    subtext: &str,
) -> std::io::Result<()> {
    let points: Vec<_> = data
        .iter()
        .map(|r| {
            let time_point = r.elapsed_time_us as f64 / 1_000_000.0; // to seconds
            let latency = r.latency_us as f64 / 1_000.0; // to milliseconds
            vec![time_point, latency]
        })
        .collect();

    // Calculate moving average with a window of 20 points
    let smoothed_points = calculate_moving_average(&points, 20);

    let title = format!("{} Latency Over Time", title_prefix);
    let stats_text = format_latency_stats(stats);
    let full_subtext = format!("{}\n{}", subtext, stats_text);

    let chart = Chart::new()
        .tooltip(Tooltip::new().trigger(Trigger::Axis))
        .axis_pointer(AxisPointer::new().type_(AxisPointerType::Cross))
        .title(
            Title::new()
                .text(title.clone())
                .subtext(full_subtext)
                .padding(5)
                .item_gap(5)
                .left("center")
                .top(5),
        )
        .grid(
            Grid::new()
                .left("10%")
                .right("15%")
                .top("10%")
                .bottom("10%"),
        )
        .x_axis(
            Axis::new()
                .name("Time (seconds)")
                .name_location(NameLocation::Center)
                .name_gap(35)
                .axis_label(AxisLabel::new().formatter("{value} s")),
        )
        .y_axis(
            Axis::new()
                .name("Latency (ms)")
                .name_location(NameLocation::Middle)
                .name_gap(50)
                .position("left")
                .axis_label(AxisLabel::new().formatter("{value} ms")),
        )
        .series(
            Line::new()
                .name("Raw Latency")
                .symbol(Symbol::None)
                .data(points)
                .line_style(LineStyle::new().opacity(0.3)), // Make raw data more transparent
        )
        .series(
            Line::new()
                .name("Moving Average (20 points)")
                .symbol(Symbol::None)
                .data(smoothed_points)
                .line_style(LineStyle::new().width(2.5)), // Make smoothed line thicker
        );

    // Save as HTML
    let mut renderer = HtmlRenderer::new(title, 1600, 1200);
    let html_path = format!("{}.html", output_path);
    info!("Generating latency HTML plot: {}", html_path);

    // Create parent directories if they don't exist
    if let Some(parent) = Path::new(&html_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    renderer.save(&chart, &html_path).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to save HTML plot: {}", e),
        )
    })?;

    Ok(())
}

fn plot_throughput_mb_over_time(
    data: &[BenchmarkRecord],
    stats: &BenchmarkActorStatistics,
    title_prefix: &str,
    output_path: &str,
    subtext: &str,
) -> std::io::Result<()> {
    // Calculate throughput per second in MB/s
    let points: Vec<_> = data
        .windows(2)
        .map(|w| {
            let time_diff = (w[1].elapsed_time_us - w[0].elapsed_time_us) as f64 / 1_000_000.0;
            let bytes_diff = (w[1].total_bytes - w[0].total_bytes) as f64;
            let throughput = bytes_diff / (1024.0 * 1024.0) / time_diff; // Convert bytes/s to MB/s
            let time_point = w[1].elapsed_time_us as f64 / 1_000_000.0;
            vec![time_point, throughput]
        })
        .collect();

    // Calculate moving average with a window of 20 points
    let smoothed_points = calculate_moving_average(&points, 20);

    let title = format!("{} Throughput Over Time", title_prefix);
    let stats_text = format_throughput_mb_stats(stats, title_prefix);
    let full_subtext = format!("{}\n{}", subtext, stats_text);

    let chart = Chart::new()
        .tooltip(Tooltip::new().trigger(Trigger::Axis))
        .axis_pointer(AxisPointer::new().type_(AxisPointerType::Cross))
        .title(
            Title::new()
                .text(title.clone())
                .subtext(full_subtext)
                .padding(5)
                .item_gap(5)
                .left("center")
                .top(5),
        )
        .grid(
            Grid::new()
                .left("10%")
                .right("15%")
                .top("10%")
                .bottom("10%"),
        )
        .x_axis(
            Axis::new()
                .name("Time (seconds)")
                .name_location(NameLocation::Center)
                .name_gap(35)
                .axis_label(AxisLabel::new().formatter("{value} s")),
        )
        .y_axis(
            Axis::new()
                .name("Megabytes per Second")
                .name_location(NameLocation::Middle)
                .name_gap(50)
                .position("left")
                .axis_label(AxisLabel::new().formatter("{value} MB/s")),
        )
        .series(
            Line::new()
                .name("Raw Throughput")
                .symbol(Symbol::None)
                .data(points)
                .line_style(LineStyle::new().opacity(0.3)), // Make raw data more transparent
        )
        .series(
            Line::new()
                .name("Moving Average (20 points)")
                .symbol(Symbol::None)
                .data(smoothed_points)
                .line_style(LineStyle::new().width(2.5)), // Make smoothed line thicker
        );

    // Save as HTML
    let mut renderer = HtmlRenderer::new(title, 1600, 1200);
    let html_path = format!("{}.html", output_path);
    info!("Generating throughput MB/s HTML plot: {}", html_path);

    // Create parent directories if they don't exist
    if let Some(parent) = Path::new(&html_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    renderer.save(&chart, &html_path).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to save HTML plot: {}", e),
        )
    })?;

    Ok(())
}
