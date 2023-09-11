use crate::streaming::users::permissioner::Permissioner;
use iggy::error::Error;

impl Permissioner {
    pub fn create_partitons(
        &self,
        user_id: u32,
        stream_id: u32,
        topic_id: u32,
    ) -> Result<(), Error> {
        self.update_topic(user_id, stream_id, topic_id)
    }

    pub fn delete_partitions(
        &self,
        user_id: u32,
        stream_id: u32,
        topic_id: u32,
    ) -> Result<(), Error> {
        self.update_topic(user_id, stream_id, topic_id)
    }
}
