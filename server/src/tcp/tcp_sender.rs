use crate::tcp::COMPONENT;
use crate::{server_error::ServerError, tcp::sender};
use error_set::ErrContext;
use iggy::error::IggyError;
use tokio::{io::AsyncWriteExt, net::TcpStream};

#[derive(Debug)]
pub struct TcpSender {
    pub(crate) stream: TcpStream,
}

impl TcpSender {
    pub async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, IggyError> {
        sender::read(&mut self.stream, buffer).await
    }

    pub async fn send_empty_ok_response(&mut self) -> Result<(), IggyError> {
        sender::send_empty_ok_response(&mut self.stream).await
    }

    pub async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), IggyError> {
        sender::send_ok_response(&mut self.stream, payload).await
    }

    pub async fn send_error_response(&mut self, error: IggyError) -> Result<(), IggyError> {
        sender::send_error_response(&mut self.stream, error).await
    }

    pub async fn shutdown(&mut self) -> Result<(), ServerError> {
        self.stream
            .shutdown()
            .await
            .with_error_context(|_| format!("{COMPONENT} - failed to shutdown tcp stream"))
            .map_err(ServerError::IoError)
    }
}
