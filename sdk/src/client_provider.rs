use crate::args::Args;
use crate::client::Client;
use crate::client_error::ClientError;
use crate::http::client::HttpClient;
use crate::http::config::HttpClientConfig;
use crate::quic::client::QuicClient;
use crate::quic::config::QuicClientConfig;
use crate::tcp::client::TcpClient;
use crate::tcp::config::TcpClientConfig;

const QUIC_TRANSPORT: &str = "quic";
const HTTP_TRANSPORT: &str = "http";
const TCP_TRANSPORT: &str = "tcp";

pub async fn get_client(args: Args) -> Result<Box<dyn Client>, ClientError> {
    match args.transport.as_str() {
        QUIC_TRANSPORT => {
            let mut client = QuicClient::create(QuicClientConfig {
                client_address: args.quic_client_address.to_string(),
                server_address: args.quic_server_address.to_string(),
                server_name: args.quic_server_name.to_string(),
                response_buffer_size: args.quic_response_buffer_size,
                max_concurrent_bidi_streams: args.quic_max_concurrent_bidi_streams,
                datagram_send_buffer_size: args.quic_datagram_send_buffer_size,
                initial_mtu: args.quic_initial_mtu,
                send_window: args.quic_send_window,
                receive_window: args.quic_receive_window,
                keep_alive_interval: args.quic_keep_alive_interval,
                max_idle_timeout: args.quic_max_idle_timeout,
            })?;
            client.connect().await?;
            Ok(Box::new(client))
        }
        HTTP_TRANSPORT => {
            let client = HttpClient::create(HttpClientConfig {
                api_url: args.http_api_url.to_string(),
                retries: args.http_retries,
            })?;
            Ok(Box::new(client))
        }
        TCP_TRANSPORT => {
            let mut client = TcpClient::create(TcpClientConfig {
                server_address: args.tcp_server_address.to_string(),
            })?;
            client.connect().await?;
            Ok(Box::new(client))
        }
        _ => Err(ClientError::InvalidTransport(args.transport)),
    }
}
