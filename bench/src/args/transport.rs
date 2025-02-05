use super::defaults::*;
use super::{output::BenchmarkOutputCommand, props::BenchmarkTransportProps};
use clap::{Parser, Subcommand};
use integration::test_server::Transport;
use serde::{Serialize, Serializer};

#[derive(Subcommand, Debug, Clone)]
pub enum BenchmarkTransportCommand {
    Http(HttpArgs),
    Tcp(TcpArgs),
    Quic(QuicArgs),
}

impl Serialize for BenchmarkTransportCommand {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let variant_str = match self {
            BenchmarkTransportCommand::Http(_) => "http",
            BenchmarkTransportCommand::Tcp(_) => "tcp",
            BenchmarkTransportCommand::Quic(_) => "quic",
        };
        serializer.serialize_str(variant_str)
    }
}

impl BenchmarkTransportProps for BenchmarkTransportCommand {
    fn transport(&self) -> &Transport {
        self.inner().transport()
    }

    fn server_address(&self) -> &str {
        self.inner().server_address()
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

    fn output_command(&self) -> &Option<BenchmarkOutputCommand> {
        self.inner().output_command()
    }
}

#[derive(Parser, Debug, Clone)]
pub struct HttpArgs {
    /// Address of the HTTP iggy-server
    #[arg(long, default_value_t = DEFAULT_HTTP_SERVER_ADDRESS.to_owned())]
    pub server_address: String,

    /// Optional output command, used to output results (charts, raw json data) to a directory
    #[command(subcommand)]
    pub output: Option<BenchmarkOutputCommand>,
}

impl BenchmarkTransportProps for HttpArgs {
    fn transport(&self) -> &Transport {
        &Transport::Http
    }

    fn server_address(&self) -> &str {
        &self.server_address
    }

    fn validate_certificate(&self) -> bool {
        panic!("Cannot validate certificate for HTTP transport!")
    }

    fn client_address(&self) -> &str {
        panic!("Setting client address for HTTP transport is not supported!")
    }

    fn output_command(&self) -> &Option<BenchmarkOutputCommand> {
        &self.output
    }
}

#[derive(Parser, Debug, Clone)]
pub struct TcpArgs {
    /// Address of the TCP iggy-server
    #[arg(long, default_value_t = DEFAULT_TCP_SERVER_ADDRESS.to_owned())]
    pub server_address: String,

    /// Optional output command, used to output results (charts, raw json data) to a directory
    #[command(subcommand)]
    output: Option<BenchmarkOutputCommand>,
}

impl BenchmarkTransportProps for TcpArgs {
    fn transport(&self) -> &Transport {
        &Transport::Tcp
    }

    fn server_address(&self) -> &str {
        &self.server_address
    }

    fn validate_certificate(&self) -> bool {
        panic!("Cannot validate certificate for TCP transport!")
    }

    fn client_address(&self) -> &str {
        panic!("Setting client address for TCP transport is not supported!")
    }

    fn output_command(&self) -> &Option<BenchmarkOutputCommand> {
        &self.output
    }
}

#[derive(Parser, Debug, Clone)]
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

    /// Optional output command, used to output results (charts, raw json data) to a directory
    #[command(subcommand)]
    pub output: Option<BenchmarkOutputCommand>,
}

impl BenchmarkTransportProps for QuicArgs {
    fn transport(&self) -> &Transport {
        &Transport::Quic
    }

    fn server_address(&self) -> &str {
        &self.server_address
    }

    fn validate_certificate(&self) -> bool {
        self.validate_certificate
    }

    fn client_address(&self) -> &str {
        &self.client_address
    }

    fn output_command(&self) -> &Option<BenchmarkOutputCommand> {
        &self.output
    }
}
