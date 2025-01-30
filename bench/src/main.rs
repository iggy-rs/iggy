mod actors;
mod analytics;
mod args;
mod benchmarks;
mod plot;
mod runner;
mod utils;

use crate::{args::common::IggyBenchArgs, runner::BenchmarkRunner};
use clap::Parser;
use figlet_rs::FIGfont;
use hostname::get;
use iggy::error::IggyError;
use std::fs;
use std::path::Path;
use tracing::{error, info};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use utils::copy_directory_contents;

#[tokio::main]
async fn main() -> Result<(), IggyError> {
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy Bench");
    println!("{}", figure.unwrap());

    let args = IggyBenchArgs::parse();
    args.validate();

    // Set default identifier as hostname if not provided
    if args.identifier().is_none() {
        info!(
            "Using hostname: {} as identifier",
            get().unwrap().to_string_lossy()
        );
    } else {
        info!("Using identifier: {}", args.identifier().unwrap());
    }

    // Store output_dir before moving args
    let output_dir = args.output_dir();
    let benchmark_dir = output_dir.as_ref().map(|dir| {
        let dir_path = Path::new(dir);
        if !dir_path.exists() {
            fs::create_dir_all(dir_path).unwrap();
        }
        dir_path.join(args.generate_dir_name())
    });

    // Configure logging
    let env_filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO"));
    let stdout_layer = fmt::layer().with_ansi(true);

    // If output directory is specified, also log to file
    if let Some(ref benchmark_dir) = benchmark_dir {
        // Create output directory if it doesn't exist
        fs::create_dir_all(benchmark_dir).unwrap();
        let file_appender = tracing_appender::rolling::never(benchmark_dir, "bench.log");
        let file_layer = fmt::layer().with_ansi(false).with_writer(file_appender);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(stdout_layer)
            .with(file_layer)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(stdout_layer)
            .init();
    }

    let mut benchmark_runner = BenchmarkRunner::new(args);
    // Store server address before running benchmark since args will be taken
    let server_addr = benchmark_runner
        .args
        .as_ref()
        .unwrap()
        .server_address()
        .to_string();

    info!("Starting the benchmarks...");
    let ctrl_c = tokio::signal::ctrl_c();
    let benchmark_future = benchmark_runner.run();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl-C, exiting...");
            // Clean up unfinished benchmark directory on manual interruption
            if let Some(ref benchmark_dir) = benchmark_dir {
                info!("Cleaning up unfinished benchmark directory...");
                if let Err(e) = std::fs::remove_dir_all(benchmark_dir) {
                    error!("Failed to clean up benchmark directory: {}", e);
                }
            }
        }
        result = benchmark_future => {
            if let Err(e) = result {
                error!("Benchmark failed with error: {:?}", e);
                return Err(e);
            }

            // Copy server logs and config if output directory is specified and
            // (server was started by benchmark OR (running on localhost and local_data exists))
            if let Some(ref benchmark_dir) = benchmark_dir {
                let is_localhost = server_addr
                    .split(':')
                    .next()
                    .map(|host| host == "localhost" || host == "127.0.0.1")
                    .unwrap_or(false);

                let local_data = Path::new("local_data");
                let should_copy = benchmark_runner.test_server.is_some() ||
                    (is_localhost && local_data.exists());

                if should_copy {
                    // Copy server logs
                    let logs_dir = local_data.join("logs");
                    if logs_dir.exists() {
                        if let Err(e) = copy_directory_contents(&logs_dir, benchmark_dir) {
                            error!("Failed to copy server logs: {}", e);
                        }
                    }

                    // Copy server config
                    let runtime_dir = local_data.join("runtime");
                    if runtime_dir.exists() {
                        if let Err(e) = copy_directory_contents(&runtime_dir, benchmark_dir) {
                            error!("Failed to copy server configuration: {}", e);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
