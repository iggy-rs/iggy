use crate::statistics::record::BenchmarkRecord;
use crate::{benchmark_result::BenchmarkInfo, statistics::actor_statistics::BenchmarkActorSummary};
use charming::element::{SplitLine, TextAlign, TextStyle};
use charming::{
    component::{
        Axis, DataView, Feature, Grid, Legend, Restore, SaveAsImage, Title, Toolbox,
        ToolboxDataZoom,
    },
    element::{
        AxisLabel, AxisPointer, AxisPointerType, AxisType, LineStyle, NameLocation, Symbol,
        Tooltip, Trigger,
    },
    series::Line,
    Chart, HtmlRenderer,
};
use std::path::Path;
use tracing::info;

fn format_throughput_stats(stats: &BenchmarkActorSummary, title_prefix: &str) -> String {
    format!(
        "Average throughput per {}: {:.3} msg/s",
        if title_prefix == "Producer" {
            "producer"
        } else {
            "consumer"
        },
        stats.throughput_messages_per_second,
    )
}

fn format_latency_stats(stats: &BenchmarkActorSummary) -> String {
    format!(
        "Average: {:.3} ms, Median: {:.3} ms, P99: {:.3} ms, P999: {:.3} ms",
        stats.avg_latency_ms, stats.median_latency_ms, stats.p99_latency_ms, stats.p999_latency_ms,
    )
}

fn format_throughput_mb_stats(stats: &BenchmarkActorSummary, title_prefix: &str) -> String {
    format!(
        "Average throughput per {}: {:.3} MB/s",
        if title_prefix == "Producer" {
            "producer"
        } else {
            "consumer"
        },
        stats.throughput_megabytes_per_second,
    )
}

pub fn generate_plots(output: &BenchmarkInfo, output_dir: &str) -> std::io::Result<()> {
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

    if let Some(overall_stats) = &output.summary {
        subtext = format!(
            "{}\nTotal throughput: {:.2} MB/s, {:.0} messages/s",
            subtext,
            overall_stats.total_throughput_megabytes_per_second,
            overall_stats.total_throughput_messages_per_second
        );
    }

    if let Some(producer_data) = &output.first_producer_raw_data {
        if let Some(producer_stats) = &output.first_producer_summary {
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
        if let Some(consumer_stats) = &output.first_consumer_summary {
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

fn plot_throughput_over_time(
    data: &[BenchmarkRecord],
    stats: &BenchmarkActorSummary,
    title_prefix: &str,
    output_path: &str,
    subtext: &str,
) -> std::io::Result<()> {
    let mut time_to_values: std::collections::BTreeMap<i64, Vec<f64>> =
        std::collections::BTreeMap::new();

    data.windows(2).for_each(|w| {
        let time_diff = (w[1].elapsed_time_us - w[0].elapsed_time_us) as f64 / 1_000_000.0;
        let messages_diff = (w[1].messages - w[0].messages) as f64;
        let throughput = ((messages_diff / time_diff) * 1000.0).round() / 1000.0;
        let time_key = (w[1].elapsed_time_us / 10_000) as i64; // Round to 0.01s
        time_to_values.entry(time_key).or_default().push(throughput);
    });

    let points: Vec<_> = time_to_values
        .into_iter()
        .map(|(time_key, values)| {
            let avg = (values.iter().sum::<f64>() / values.len() as f64 * 1000.0).round() / 1000.0;
            vec![(time_key as f64 / 100.0), avg]
        })
        .collect();

    let window_size = 50;
    let half_window = window_size / 2;
    let throughputs: Vec<f64> = points.iter().map(|p| p[1]).collect();
    let mut smoothed_throughputs = vec![0.0; throughputs.len()];

    (0..throughputs.len()).for_each(|i| {
        let start = i.saturating_sub(half_window);
        let end = if i + half_window >= throughputs.len() {
            throughputs.len()
        } else {
            i + half_window + 1
        };
        let window = &throughputs[start..end];
        smoothed_throughputs[i] =
            (window.iter().sum::<f64>() / window.len() as f64 * 1000.0).round() / 1000.0;
    });

    let smoothed_points: Vec<Vec<f64>> = points
        .iter()
        .zip(smoothed_throughputs)
        .map(|(p, s)| vec![p[0], s])
        .collect();

    let title = format!("{} Throughput Over Time", title_prefix);
    let stats_text = format_throughput_stats(stats, title_prefix);
    let full_subtext = format!("{}\n{}", subtext, stats_text);

    let chart = Chart::new()
        .tooltip(
            Tooltip::new()
                .trigger(Trigger::Axis)
                .axis_pointer(AxisPointer::new().type_(AxisPointerType::Cross)),
        )
        .legend(Legend::new().show(true).bottom("5%"))
        .toolbox(
            Toolbox::new().feature(
                Feature::new()
                    .data_zoom(ToolboxDataZoom::new())
                    .data_view(DataView::new())
                    .restore(Restore::new())
                    .save_as_image(SaveAsImage::new()),
            ),
        )
        .title(
            Title::new()
                .text(title.clone())
                .subtext(full_subtext)
                .text_align(TextAlign::Center)
                .subtext_style(TextStyle::new().font_size(14))
                .padding(25)
                .item_gap(8)
                .left("50%")
                .top("5%"),
        )
        .grid(
            Grid::new()
                .left("10%")
                .right("10%")
                .top("20%")
                .bottom("10%")
                .contain_label(true),
        )
        .x_axis(
            Axis::new()
                .type_(AxisType::Value)
                .name("Time (seconds)")
                .name_location(NameLocation::Center)
                .name_gap(35)
                .axis_label(AxisLabel::new().formatter("{value} s"))
                .split_line(SplitLine::new().show(true)),
        )
        .y_axis(
            Axis::new()
                .type_(AxisType::Value)
                .name("Messages per Second")
                .name_location(NameLocation::End)
                .name_gap(15)
                .name_rotation(0)
                .position("left")
                .axis_label(AxisLabel::new().formatter("{value} msg/s"))
                .split_line(SplitLine::new().show(true)),
        )
        .series(
            Line::new()
                .name("Raw Throughput")
                .symbol(Symbol::None)
                .data(points.clone())
                .line_style(LineStyle::new().opacity(0.3)),
        )
        .series(
            Line::new()
                .name("Moving Average (50 points)")
                .symbol(Symbol::None)
                .data(smoothed_points)
                .line_style(LineStyle::new().width(2.5)),
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
    stats: &BenchmarkActorSummary,
    title_prefix: &str,
    output_path: &str,
    subtext: &str,
) -> std::io::Result<()> {
    let mut time_to_values: std::collections::BTreeMap<i64, Vec<f64>> =
        std::collections::BTreeMap::new();

    data.windows(2).for_each(|w| {
        let latency = (w[1].latency_us as f64 / 1_000.0 * 1000.0).round() / 1000.0;
        let time_key = (w[1].elapsed_time_us / 10_000) as i64; // Round to 0.01s
        time_to_values.entry(time_key).or_default().push(latency);
    });

    let points: Vec<_> = time_to_values
        .into_iter()
        .map(|(time_key, values)| {
            let avg = (values.iter().sum::<f64>() / values.len() as f64 * 1000.0).round() / 1000.0;
            vec![(time_key as f64 / 100.0), avg]
        })
        .collect();

    let window_size = 50;
    let half_window = window_size / 2;
    let latencies: Vec<f64> = points.iter().map(|p| p[1]).collect();
    let mut smoothed_latencies = vec![0.0; latencies.len()];

    (0..latencies.len()).for_each(|i| {
        let start = i.saturating_sub(half_window);
        let end = if i + half_window >= latencies.len() {
            latencies.len()
        } else {
            i + half_window + 1
        };
        let window = &latencies[start..end];
        smoothed_latencies[i] =
            (window.iter().sum::<f64>() / window.len() as f64 * 1000.0).round() / 1000.0;
    });

    let smoothed_points: Vec<Vec<f64>> = points
        .iter()
        .zip(smoothed_latencies)
        .map(|(p, s)| vec![p[0], s])
        .collect();

    let title = format!("{} Latency Over Time", title_prefix);
    let stats_text = format_latency_stats(stats);
    let full_subtext = format!("{}\n{}", subtext, stats_text);

    let chart = Chart::new()
        .tooltip(
            Tooltip::new()
                .trigger(Trigger::Axis)
                .axis_pointer(AxisPointer::new().type_(AxisPointerType::Cross)),
        )
        .legend(Legend::new().show(true).bottom("5%"))
        .toolbox(
            Toolbox::new().feature(
                Feature::new()
                    .data_zoom(ToolboxDataZoom::new())
                    .data_view(DataView::new())
                    .restore(Restore::new())
                    .save_as_image(SaveAsImage::new()),
            ),
        )
        .title(
            Title::new()
                .text(title.clone())
                .subtext(full_subtext)
                .text_align(TextAlign::Center)
                .subtext_style(TextStyle::new().font_size(14))
                .padding(25)
                .item_gap(8)
                .left("50%")
                .top("5%"),
        )
        .grid(
            Grid::new()
                .left("10%")
                .right("10%")
                .top("20%")
                .bottom("10%")
                .contain_label(true),
        )
        .x_axis(
            Axis::new()
                .type_(AxisType::Value)
                .name("Time (seconds)")
                .name_location(NameLocation::Center)
                .name_gap(35)
                .axis_label(AxisLabel::new().formatter("{value} s"))
                .split_line(SplitLine::new().show(true)),
        )
        .y_axis(
            Axis::new()
                .type_(AxisType::Value)
                .name("Latency (ms)")
                .name_location(NameLocation::End)
                .name_gap(15)
                .name_rotation(0)
                .position("left")
                .axis_label(AxisLabel::new().formatter("{value} ms"))
                .split_line(SplitLine::new().show(true)),
        )
        .series(
            Line::new()
                .name("Raw Latency")
                .symbol(Symbol::None)
                .data(points.clone())
                .line_style(LineStyle::new().opacity(0.3)),
        )
        .series(
            Line::new()
                .name("Moving Average (50 points)")
                .symbol(Symbol::None)
                .data(smoothed_points)
                .line_style(LineStyle::new().width(2.5)),
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
    stats: &BenchmarkActorSummary,
    title_prefix: &str,
    output_path: &str,
    subtext: &str,
) -> std::io::Result<()> {
    let mut time_to_values: std::collections::BTreeMap<i64, Vec<f64>> =
        std::collections::BTreeMap::new();

    data.windows(2).for_each(|w| {
        let time_diff = (w[1].elapsed_time_us - w[0].elapsed_time_us) as f64 / 1_000_000.0;
        let bytes_diff = (w[1].total_bytes - w[0].total_bytes) as f64;
        let throughput = ((bytes_diff / (1024.0 * 1024.0) / time_diff) * 1000.0).round() / 1000.0;
        let time_key = (w[1].elapsed_time_us / 10_000) as i64; // Round to 0.01s
        time_to_values.entry(time_key).or_default().push(throughput);
    });

    let points: Vec<_> = time_to_values
        .into_iter()
        .map(|(time_key, values)| {
            let avg = (values.iter().sum::<f64>() / values.len() as f64 * 1000.0).round() / 1000.0;
            vec![(time_key as f64 / 100.0), avg]
        })
        .collect();

    let window_size = 50;
    let half_window = window_size / 2;
    let throughputs: Vec<f64> = points.iter().map(|p| p[1]).collect();
    let mut smoothed_throughputs = vec![0.0; throughputs.len()];

    (0..throughputs.len()).for_each(|i| {
        let start = i.saturating_sub(half_window);
        let end = if i + half_window >= throughputs.len() {
            throughputs.len()
        } else {
            i + half_window + 1
        };
        let window = &throughputs[start..end];
        smoothed_throughputs[i] =
            (window.iter().sum::<f64>() / window.len() as f64 * 1000.0).round() / 1000.0;
    });

    let smoothed_points: Vec<Vec<f64>> = points
        .iter()
        .zip(smoothed_throughputs)
        .map(|(p, s)| vec![p[0], s])
        .collect();

    let title = format!("{} Throughput Over Time", title_prefix);
    let stats_text = format_throughput_mb_stats(stats, title_prefix);
    let full_subtext = format!("{}\n{}", subtext, stats_text);

    let chart = Chart::new()
        .tooltip(
            Tooltip::new()
                .trigger(Trigger::Axis)
                .axis_pointer(AxisPointer::new().type_(AxisPointerType::Cross)),
        )
        .legend(Legend::new().show(true).bottom("5%"))
        .toolbox(
            Toolbox::new().feature(
                Feature::new()
                    .data_zoom(ToolboxDataZoom::new())
                    .data_view(DataView::new())
                    .restore(Restore::new())
                    .save_as_image(SaveAsImage::new()),
            ),
        )
        .title(
            Title::new()
                .text(title.clone())
                .subtext(full_subtext)
                .text_align(TextAlign::Center)
                .subtext_style(TextStyle::new().font_size(14))
                .padding(25)
                .item_gap(8)
                .left("50%")
                .top("5%"),
        )
        .grid(
            Grid::new()
                .left("10%")
                .right("10%")
                .top("20%")
                .bottom("10%")
                .contain_label(true),
        )
        .x_axis(
            Axis::new()
                .type_(AxisType::Value)
                .name("Time (seconds)")
                .name_location(NameLocation::Center)
                .name_gap(35)
                .axis_label(AxisLabel::new().formatter("{value} s"))
                .split_line(SplitLine::new().show(true)),
        )
        .y_axis(
            Axis::new()
                .type_(AxisType::Value)
                .name("Megabytes per Second")
                .name_location(NameLocation::End)
                .name_gap(15)
                .name_rotation(0)
                .position("left")
                .axis_label(AxisLabel::new().formatter("{value} MB/s"))
                .split_line(SplitLine::new().show(true)),
        )
        .series(
            Line::new()
                .name("Raw Throughput")
                .symbol(Symbol::None)
                .data(points.clone())
                .line_style(LineStyle::new().opacity(0.3)),
        )
        .series(
            Line::new()
                .name("Moving Average (50 points)")
                .symbol(Symbol::None)
                .data(smoothed_points)
                .line_style(LineStyle::new().width(2.5)),
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
