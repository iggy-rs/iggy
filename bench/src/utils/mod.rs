use iggy::{
    client::{Client, SystemClient, UserClient},
    clients::builder::IggyClientBuilder,
    error::IggyError,
    models::stats::Stats,
    snapshot::{SnapshotCompression, SystemSnapshotType},
    users::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME},
};
use iggy_bench_report::transport::BenchmarkTransport;
use std::{fs, path::Path};
use tracing::error;

pub mod client_factory;
pub mod server_starter;

pub async fn get_server_stats(
    transport: &BenchmarkTransport,
    server_address: &str,
) -> Result<Stats, IggyError> {
    let client = IggyClientBuilder::new();

    let client = match transport {
        BenchmarkTransport::Tcp => client
            .with_tcp()
            .with_server_address(server_address.to_string())
            .build()?,
        BenchmarkTransport::Http => client
            .with_http()
            .with_api_url(format!("http://{}", server_address))
            .build()?,
        BenchmarkTransport::Quic => client
            .with_quic()
            .with_server_address(server_address.to_string())
            .build()?,
    };

    client.connect().await?;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;

    client.get_stats().await
}

pub async fn collect_server_logs_and_save_to_file(
    transport: &BenchmarkTransport,
    server_address: &str,
    output_dir: &Path,
) -> Result<(), IggyError> {
    let client = IggyClientBuilder::new();

    let client = match transport {
        BenchmarkTransport::Tcp => client
            .with_tcp()
            .with_server_address(server_address.to_string())
            .build()?,
        BenchmarkTransport::Http => client
            .with_http()
            .with_api_url(format!("http://{}", server_address))
            .build()?,
        BenchmarkTransport::Quic => client
            .with_quic()
            .with_server_address(server_address.to_string())
            .build()?,
    };

    client.connect().await?;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;

    let snapshot = client
        .snapshot(
            SnapshotCompression::Deflated,
            vec![SystemSnapshotType::ServerLogs],
        )
        .await?
        .0;

    fs::write(output_dir.join("server_logs.zip"), snapshot).map_err(|e| {
        error!("Failed to write server logs to file: {:?}", e);
        IggyError::CannotWriteToFile
    })
}
