use crate::binary::sender::Sender;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use iggy::error::IggyError;
use quinn::{RecvStream, SendStream};
use std::mem::size_of;
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
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, IggyError> {
        let read_bytes = self.recv.read(buffer).await;
        if let Err(error) = read_bytes {
            return Err(IggyError::from(error));
        }

        Ok(read_bytes.unwrap().unwrap())
    }

    async fn send_empty_ok_response(&mut self) -> Result<(), IggyError> {
        self.send_ok_response(&[]).await
    }

    async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), IggyError> {
        self.send_response(STATUS_OK, payload).await
    }

    async fn send_error_response(&mut self, error: IggyError) -> Result<(), IggyError> {
        let error_message = error.to_string();
        let length = error_message.len() as u32;

        let mut error_details_buffer =
            BytesMut::with_capacity(error_message.len() + size_of::<u32>());
        error_details_buffer.put_u32_le(length);
        error_details_buffer.put_slice(error_message.as_bytes());

        self.send_response(&error.as_code().to_le_bytes(), &error_details_buffer)
            .await
    }
}

impl QuicSender {
    async fn send_response(&mut self, status: &[u8], payload: &[u8]) -> Result<(), IggyError> {
        debug!("Sending response with status: {:?}...", status);
        let length = (payload.len() as u32).to_le_bytes();
        self.send
            .write_all(&[status, &length, payload].as_slice().concat())
            .await?;
        self.send.finish()?;
        debug!("Sent response with status: {:?}", status);
        Ok(())
    }
}
