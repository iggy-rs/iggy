use crate::args::common::IggyBenchArgs;
use integration::http_client::HttpClientFactory;
use integration::quic_client::QuicClientFactory;
use integration::tcp_client::TcpClientFactory;
use integration::test_server::{ClientFactory, Transport};
use std::sync::Arc;

pub fn create_client_factory(args: &IggyBenchArgs) -> Arc<dyn ClientFactory> {
    match &args.transport() {
        Transport::Http => Arc::new(HttpClientFactory {
            server_addr: args.server_address().to_owned(),
        }),
        Transport::Tcp => Arc::new(TcpClientFactory {
            server_addr: args.server_address().to_owned(),
        }),
        Transport::Quic => Arc::new(QuicClientFactory {
            server_addr: args.server_address().to_owned(),
        }),
    }
}
