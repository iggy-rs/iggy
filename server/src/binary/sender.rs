use iggy::error::IggyError;
use monoio::buf::{IoBuf, IoBufMut};

pub trait Sender: Sync + Send {
    async fn read(&mut self, buffer: impl IoBuf + IoBufMut + Unpin + 'static) -> (Result<usize, IggyError>, impl IoBufMut + IoBuf);
    async fn send_empty_ok_response(&mut self) -> Result<(), IggyError>;
    async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), IggyError>;
    async fn send_error_response(&mut self, error: IggyError) -> Result<(), IggyError>;
}
