use crate::binary::sender::Sender;
use async_trait::async_trait;
use quinn::SendStream;
use shared::error::Error;
use tracing::trace;

const STATUS_OK: &[u8] = &[0];

#[derive(Debug)]
pub struct QuicSender {
    pub(crate) send: SendStream,
}

unsafe impl Send for QuicSender {}
unsafe impl Sync for QuicSender {}

#[async_trait]
impl Sender for QuicSender {
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

impl QuicSender {
    async fn send_response(&mut self, status: &[u8], payload: &[u8]) -> Result<(), Error> {
        trace!("Sending response with status: {:?}...", status);
        let length = (payload.len() as u32).to_le_bytes();
        self.send
            .write_all(&[status, &length, payload].as_slice().concat())
            .await?;
        self.send.finish().await?;
        trace!("Sent response with status: {:?}", status);
        Ok(())
    }
}
