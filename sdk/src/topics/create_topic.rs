use crate::client::Client;
use crate::error::Error;

const COMMAND: &[u8] = &[21];

impl Client {
    pub async fn create_topic(
        &mut self,
        stream_id: u32,
        topic_id: u32,
        partitions_count: u32,
        name: &str,
    ) -> Result<(), Error> {
        if partitions_count > 100 {
            return Err(Error::TooManyPartitions);
        }

        if name.len() > 100 {
            return Err(Error::InvalidTopicName);
        }

        let stream_id = &stream_id.to_le_bytes();
        let topic_id = &topic_id.to_le_bytes();
        let partitions_count = &partitions_count.to_le_bytes();
        let name = name.as_bytes();

        self.send(
            [COMMAND, stream_id, topic_id, partitions_count, name]
                .concat()
                .as_slice(),
        )
        .await
    }
}
