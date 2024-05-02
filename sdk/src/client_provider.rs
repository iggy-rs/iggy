use crate::client::Client;
use crate::client_error::ClientError;
#[allow(deprecated)]
use crate::clients::client::IggyClient;
use crate::http::client::HttpClient;
use crate::http::config::HttpClientConfig;
use crate::tcp::client::TcpClient;
use crate::tcp::config::TcpClientConfig;
use std::sync::Arc;

const HTTP_TRANSPORT: &str = "http";
const TCP_TRANSPORT: &str = "tcp";

/// Configuration for the `ClientProvider`.
/// It consists of the following fields:
/// - `transport`: the transport to use. Valid values are `http` and `tcp`.
/// - `http`: the optional configuration for the HTTP transport.
/// - `tcp`: the optional configuration for the TCP transport.
#[derive(Debug)]
pub struct ClientProviderConfig {
    /// The transport to use. Valid values are `http` and `tcp`.
    pub transport: String,
    /// The optional configuration for the HTTP transport.
    pub http: Option<Arc<HttpClientConfig>>,
    /// The optional configuration for the TCP transport.
    pub tcp: Option<Arc<TcpClientConfig>>,
}

impl Default for ClientProviderConfig {
    fn default() -> ClientProviderConfig {
        ClientProviderConfig {
            transport: TCP_TRANSPORT.to_string(),
            http: Some(Arc::new(HttpClientConfig::default())),
            tcp: Some(Arc::new(TcpClientConfig::default())),
        }
    }
}

impl ClientProviderConfig {
    /// Create a new `ClientProviderConfig` from the provided `Args`.
    pub fn from_args(args: crate::args::Args) -> Result<Self, ClientError> {
        let transport = args.transport;
        let mut config = Self {
            transport,
            http: None,
            tcp: None,
        };
        match config.transport.as_str() {
            HTTP_TRANSPORT => {
                config.http = Some(Arc::new(HttpClientConfig {
                    api_url: args.http_api_url,
                    retries: args.http_retries,
                }));
            }
            TCP_TRANSPORT => {
                config.tcp = Some(Arc::new(TcpClientConfig {
                    server_address: args.tcp_server_address,
                    reconnection_retries: args.tcp_reconnection_retries,
                    reconnection_interval: args.tcp_reconnection_interval,
                    tls_enabled: args.tcp_tls_enabled,
                    tls_domain: args.tcp_tls_domain,
                }));
            }
            _ => return Err(ClientError::InvalidTransport(config.transport.clone())),
        }

        Ok(config)
    }
}

/// Create a default `IggyClient` with the default configuration.
pub async fn get_default_client_() -> Result<IggyClient, ClientError> {
    get_client(Arc::new(ClientProviderConfig::default())).await
}

/// Create a `IggyClient` for the specific transport based on the provided configuration.
pub async fn get_client(config: Arc<ClientProviderConfig>) -> Result<IggyClient, ClientError> {
    let client = get_raw_connected_client(config).await?;
    Ok(IggyClient::builder().with_client(client).build()?)
}

/// Create a `Client` for the specific transport based on the provided configuration.
pub async fn get_raw_connected_client(
    config: Arc<ClientProviderConfig>,
) -> Result<Box<dyn Client>, ClientError> {
    get_raw_client(config, true).await
}

/// Create a `Client` for the specific transport based on the provided configuration.
pub async fn get_raw_client(
    config: Arc<ClientProviderConfig>,
    establish_connection: bool,
) -> Result<Box<dyn Client>, ClientError> {
    let transport = config.transport.clone();
    match transport.as_str() {
        HTTP_TRANSPORT => {
            let http_config = config.http.as_ref().unwrap();
            let client = HttpClient::create(http_config.clone())?;
            Ok(Box::new(client))
        }
        TCP_TRANSPORT => {
            let tcp_config = config.tcp.as_ref().unwrap();
            let client = TcpClient::create(tcp_config.clone())?;
            if establish_connection {
                Client::connect(&client).await?
            };
            Ok(Box::new(client))
        }
        _ => Err(ClientError::InvalidTransport(transport)),
    }
}
