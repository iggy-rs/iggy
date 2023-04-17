use crate::client::Client;
use crate::error::Error;

const COMMAND: &[u8] = &[12];

impl Client {
    pub async fn delete_stream(&mut self, stream_id: u32) -> Result<(), Error> {
        let stream_id = &stream_id.to_le_bytes();
        self.send([COMMAND, stream_id].concat().as_slice()).await
    }
}
