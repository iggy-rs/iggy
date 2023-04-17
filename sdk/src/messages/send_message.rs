use crate::client::Client;
use crate::error::Error;

const COMMAND: &[u8] = &[3];

impl Client {
    pub async fn send_message(
        &mut self,
        stream_id: u32,
        topic_id: u32,
        key_kind: u8,
        key_value: u32,
        payload: &[u8],
    ) -> Result<(), Error> {
        if payload.len() > 1000 {
            return Err(Error::TooBigPayload);
        }

        let stream_id = &stream_id.to_le_bytes();
        let topic_id = &topic_id.to_le_bytes();
        let key_kind = &key_kind.to_le_bytes();
        let key_value = &key_value.to_le_bytes();
        self.send(
            [COMMAND, stream_id, topic_id, key_kind, key_value, payload]
                .concat()
                .as_slice(),
        )
        .await
    }
}
