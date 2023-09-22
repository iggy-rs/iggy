use crate::binary::sender::Sender;
use async_trait::async_trait;
use iggy::error::Error;
use quinn::{RecvStream, SendStream};
use tracing::debug;

const STATUS_OK: &[u8] = &[0; 4];

#[derive(Debug)]
pub struct QuicSender {
    pub(crate) send: SendStream,
    pub(crate) recv: RecvStream,
}

unsafe impl Send for QuicSender {}
unsafe impl Sync for QuicSender {}

#[async_trait]
impl Sender for QuicSender {
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Error> {
        let read_bytes = self.recv.read(buffer).await;
        if let Err(error) = read_bytes {
            return Err(Error::from(error));
        }

        Ok(read_bytes.unwrap().unwrap())
    }

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
        debug!("Sending response with status: {:?}...", status);
        let length = (payload.len() as u32).to_le_bytes();
        self.send
            .write_all(&[status, &length, payload].as_slice().concat())
            .await?;
        self.send.finish().await?;
        debug!("Sent response with status: {:?}", status);
        Ok(())
    }
}
