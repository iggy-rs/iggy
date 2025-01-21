use crate::{quic::quic_sender::QuicSender, server_error::ServerError};
use crate::tcp::tcp_sender::TcpSender;
use crate::tcp::tcp_tls_sender::TcpTlsSender;
use iggy::error::IggyError;
use tokio::net::TcpStream;
use tokio_native_tls::TlsStream;
use quinn::{RecvStream, SendStream};

pub enum SenderKind {
    Tcp(TcpSender),
    TcpTls(TcpTlsSender),
    Quic(QuicSender),
}

impl SenderKind {
    pub fn get_tcp_sender(stream: TcpStream) -> Self {
        Self::Tcp(TcpSender{stream})
    }

    pub fn get_tcp_tls_sender(stream: TlsStream<TcpStream>) -> Self {
        Self::TcpTls(TcpTlsSender{stream})
    }

    pub fn get_quic_sender(send_stream: SendStream, recv_stream: RecvStream) -> Self {
        Self::Quic(QuicSender{send: send_stream, recv: recv_stream})
    }

    pub async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, IggyError> {
        match self {
            Self::Tcp(s) => s.read(buffer).await,
            Self::TcpTls(s) => s.read(buffer).await,
            Self::Quic(s) => s.read(buffer).await,
        }
    }

    pub async fn send_empty_ok_response(&mut self) -> Result<(), IggyError> {
        match self {
            Self::Tcp(s) => s.send_empty_ok_response().await,
            Self::TcpTls(s) => s.send_empty_ok_response().await,
            Self::Quic(s) => s.send_empty_ok_response().await,
        }
    }

    pub async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), IggyError> {
        match self {
            Self::Tcp(s) => s.send_ok_response(payload).await,
            Self::TcpTls(s) => s.send_ok_response(payload).await,
            Self::Quic(s) => s.send_ok_response(payload).await,
        }
    }

    pub async fn send_error_response(&mut self, error: IggyError) -> Result<(), IggyError> {
        match self {
            Self::Tcp(s) => s.send_error_response(error).await,
            Self::TcpTls(s) => s.send_error_response(error).await,
            Self::Quic(s) => s.send_error_response(error).await,
        }
    }

    pub async fn shutdown(&mut self) -> Result<(), ServerError> {
        match self {
            Self::Tcp(s) => s.shutdown().await,
            Self::TcpTls(s) => s.shutdown().await,
            Self::Quic(_) => Ok(())
        }
    }
}
