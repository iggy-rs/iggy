use crate::binary::sender::Sender;
use crate::tcp::sender;
use iggy::error::IggyError;
use monoio::buf::IoBufMut;
use monoio::net::TcpStream;

#[derive(Debug)]
pub struct TcpSender {
    pub(crate) stream: TcpStream,
}

unsafe impl Send for TcpSender {}
unsafe impl Sync for TcpSender {}

impl Sender for TcpSender {
    async fn read(&mut self, buffer: impl IoBufMut + Unpin + 'static) -> (Result<usize, IggyError>, impl IoBufMut) {
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
