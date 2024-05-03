use iggy::error::IggyError;

pub trait Sender: Sync + Send {
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, IggyError>;
    async fn send_empty_ok_response(&mut self) -> Result<(), IggyError>;
    async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), IggyError>;
    async fn send_error_response(&mut self, error: IggyError) -> Result<(), IggyError>;
}
