use crate::client::Client;
use crate::error::Error;

const COMMAND: &[u8] = &[22];

impl Client {
    pub async fn delete_topic(&mut self, stream_id: u32, topic_id: u32) -> Result<(), Error> {
        let stream_id = &stream_id.to_le_bytes();
        let topic_id = &topic_id.to_le_bytes();
        self.send([COMMAND, stream_id, topic_id].concat().as_slice())
            .await
    }
}
