use quinn::SendStream;
use shared::error::Error;
use tracing::trace;

const STATUS_OK: &[u8] = &[0];

pub struct Sender {
    pub send: SendStream,
}

impl Sender {
    pub async fn send_empty_ok_response(&mut self) -> Result<(), Error> {
        self.send_ok_response(&[]).await
    }

    pub async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), Error> {
        self.send_response(STATUS_OK, payload).await
    }

    pub async fn send_error_response(&mut self, error: Error) -> Result<(), Error> {
        self.send_response(&error.code().to_le_bytes(), &[]).await
    }

    async fn send_response(&mut self, status: &[u8], payload: &[u8]) -> Result<(), Error> {
        trace!("Sending response with status: {:?}...", status);
        self.send
            .write_all(&[status, payload].as_slice().concat())
            .await?;
        self.send.finish().await?;
        trace!("Sent response with status: {:?}", status);
        Ok(())
    }
}
