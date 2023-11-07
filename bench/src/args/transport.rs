use super::defaults::*;
use super::props::BenchmarkTransportProps;
use clap::{Parser, Subcommand};
use integration::test_server::Transport;
use std::num::NonZeroU32;

#[derive(Subcommand, Debug)]
pub enum BenchmarkTransportCommand {
    Http(HttpArgs),
    Tcp(TcpArgs),
    Quic(QuicArgs),
}

impl BenchmarkTransportProps for BenchmarkTransportCommand {
    fn transport(&self) -> &Transport {
        self.inner().transport()
    }

    fn server_address(&self) -> &str {
        self.inner().server_address()
    }

    fn start_stream_id(&self) -> u32 {
        self.inner().start_stream_id()
    }

    fn validate_certificate(&self) -> bool {
        self.inner().validate_certificate()
    }

    fn client_address(&self) -> &str {
        self.inner().client_address()
    }

    fn inner(&self) -> &dyn BenchmarkTransportProps {
        match self {
            BenchmarkTransportCommand::Http(args) => args,
            BenchmarkTransportCommand::Tcp(args) => args,
            BenchmarkTransportCommand::Quic(args) => args,
        }
    }
}

#[derive(Parser, Debug)]
pub struct HttpArgs {
    /// Address of the HTTP iggy-server
    #[arg(long, default_value_t = DEFAULT_HTTP_SERVER_ADDRESS.to_owned())]
    pub server_address: String,

    /// Start stream id
    #[arg(long, default_value_t = DEFAULT_HTTP_START_STREAM_ID)]
    pub start_stream_id: NonZeroU32,
}

impl BenchmarkTransportProps for HttpArgs {
    fn transport(&self) -> &Transport {
        &Transport::Http
    }

    fn server_address(&self) -> &str {
        &self.server_address
    }

    fn start_stream_id(&self) -> u32 {
        self.start_stream_id.get()
    }

    fn validate_certificate(&self) -> bool {
        panic!("Cannot validate certificate for HTTP transport!")
    }

    fn client_address(&self) -> &str {
        panic!("Setting client address for HTTP transport is not supported!")
    }
}

#[derive(Parser, Debug)]
pub struct TcpArgs {
    /// Address of the TCP iggy-server
    #[arg(long, default_value_t = DEFAULT_TCP_SERVER_ADDRESS.to_owned())]
    pub server_address: String,

    /// Start stream id
    #[arg(long, default_value_t = DEFAULT_TCP_START_STREAM_ID)]
    pub start_stream_id: NonZeroU32,
}

impl BenchmarkTransportProps for TcpArgs {
    fn transport(&self) -> &Transport {
        &Transport::Tcp
    }

    fn server_address(&self) -> &str {
        &self.server_address
    }

    fn start_stream_id(&self) -> u32 {
        self.start_stream_id.get()
    }

    fn validate_certificate(&self) -> bool {
        panic!("Cannot validate certificate for TCP transport!")
    }

    fn client_address(&self) -> &str {
        panic!("Setting client address for TCP transport is not supported!")
    }
}

#[derive(Parser, Debug)]
pub struct QuicArgs {
    /// Address to which the QUIC client will bind
    #[arg(long, default_value_t = DEFAULT_QUIC_CLIENT_ADDRESS.to_owned())]
    pub client_address: String,

    /// Address of the QUIC server
    #[arg(long, default_value_t = DEFAULT_QUIC_SERVER_ADDRESS.to_owned())]
    pub server_address: String,

    /// Server name
    #[arg(long, default_value_t = DEFAULT_QUIC_SERVER_NAME.to_owned())]
    pub server_name: String,

    /// Flag, enables certificate validation
    #[arg(long, default_value_t = DEFAULT_QUIC_VALIDATE_CERTIFICATE)]
    pub validate_certificate: bool,

    /// Start stream id
    #[arg(long, default_value_t = DEFAULT_QUIC_START_STREAM_ID)]
    pub start_stream_id: NonZeroU32,
}

impl BenchmarkTransportProps for QuicArgs {
    fn transport(&self) -> &Transport {
        &Transport::Quic
    }

    fn server_address(&self) -> &str {
        &self.server_address
    }

    fn start_stream_id(&self) -> u32 {
        self.start_stream_id.get()
    }

    fn validate_certificate(&self) -> bool {
        self.validate_certificate
    }

    fn client_address(&self) -> &str {
        &self.client_address
    }
}
