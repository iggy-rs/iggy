use crate::binary::sender::Sender;
use async_trait::async_trait;
use iggy::error::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::trace;

const STATUS_OK: &[u8] = &[0];

#[derive(Debug)]
pub struct TcpSender {
    pub(crate) stream: TcpStream,
}

unsafe impl Send for TcpSender {}
unsafe impl Sync for TcpSender {}

#[async_trait]
impl Sender for TcpSender {
    async fn send_empty_ok_response(&mut self) -> Result<(), Error> {
        self.send_ok_response(&[]).await
    }

    async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), Error> {
        self.send_response(STATUS_OK, payload).await
    }

    async fn send_error_response(&mut self, error: Error) -> Result<(), Error> {
        self.send_response(&error.as_code().to_le_bytes(), &[])
            .await
    }
}

impl TcpSender {
    async fn send_response(&mut self, status: &[u8], payload: &[u8]) -> Result<(), Error> {
        trace!("Sending response with status: {:?}...", status);
        let length = (payload.len() as u32).to_le_bytes();
        self.stream
            .write_all(&[status, &length, payload].as_slice().concat())
            .await?;
        trace!("Sent response with status: {:?}", status);
        Ok(())
    }
}
