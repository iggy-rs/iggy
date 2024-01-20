use crate::binary::sender::Sender;
use crate::tcp::sender;
use async_trait::async_trait;
use iggy::error::IggyError;
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct TcpSender {
    pub(crate) stream: TcpStream,
}

unsafe impl Send for TcpSender {}
unsafe impl Sync for TcpSender {}

#[async_trait]
impl Sender for TcpSender {
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, IggyError> {
        sender::read(&mut self.stream, buffer).await
    }

    async fn send_empty_ok_response(&mut self) -> Result<(), IggyError> {
        sender::send_empty_ok_response(&mut self.stream).await
    }

    async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), IggyError> {
        sender::send_ok_response(&mut self.stream, payload).await
    }

    async fn send_error_response(&mut self, error: IggyError) -> Result<(), IggyError> {
        sender::send_error_response(&mut self.stream, error).await
    }
}
