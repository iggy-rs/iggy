mod args;
mod benchmark_result;
mod benchmark_runner;
mod benchmarks;
mod client_factory;
mod consumer;
mod producer;
mod server_starter;

use crate::{args::common::IggyBenchArgs, benchmark_runner::BenchmarkRunner};
use clap::Parser;
use figlet_rs::FIGfont;
use iggy::error::IggyError;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), IggyError> {
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy Bench");
    println!("{}", figure.unwrap());
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .with_ansi(true)
        .init();

    let args = IggyBenchArgs::parse();
    args.validate();

    let mut benchmark_runner = BenchmarkRunner::new(args);

    info!("Starting the benchmarks...");
    let ctrl_c = tokio::signal::ctrl_c();
    let benchmark_future = benchmark_runner.run();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl-C, exiting...");
        }
        result = benchmark_future => {
            if let Err(e) = result {
                error!("Benchmark failed with error: {:?}", e);
                return Err(e);
            }
            info!("Finished the benchmarks");
        }
    }

    Ok(())
}
