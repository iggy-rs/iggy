use iggy::{
    client::{Client, SystemClient, UserClient},
    clients::builder::IggyClientBuilder,
    error::IggyError,
    users::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME},
};
use iggy_benchmark_report::{params::BenchmarkParams, transport::BenchmarkTransport};

pub async fn get_server_version(params: &BenchmarkParams) -> Result<String, IggyError> {
    let client = IggyClientBuilder::new();

    let client = match params.transport {
        BenchmarkTransport::Tcp => client
            .with_tcp()
            .with_server_address(params.server_address.to_string())
            .build()?,
        BenchmarkTransport::Http => client
            .with_http()
            .with_api_url(format!("http://{}", params.server_address))
            .build()?,
        BenchmarkTransport::Quic => client
            .with_quic()
            .with_server_address(params.server_address.to_string())
            .build()?,
    };

    client.connect().await?;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;

    Ok(client.get_stats().await?.iggy_server_version)
}
