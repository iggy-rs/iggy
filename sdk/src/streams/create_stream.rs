use crate::client::Client;
use crate::error::Error;

const COMMAND: &[u8] = &[11];

impl Client {
    pub async fn create_stream(&mut self, stream_id: u32, name: &str) -> Result<(), Error> {
        let stream_id = &stream_id.to_le_bytes();
        let name = name.as_bytes();
        if name.len() > 100 {
            return Err(Error::InvalidStreamName);
        }

        self.send([COMMAND, stream_id, name].concat().as_slice())
            .await
    }
}
