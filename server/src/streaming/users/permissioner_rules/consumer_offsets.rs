use crate::streaming::users::permissioner::Permissioner;
use iggy::error::IggyError;

impl Permissioner {
    pub fn get_consumer_offset(
        &self,
        user_id: u32,
        stream_id: u32,
        topic_id: u32,
    ) -> Result<(), IggyError> {
        self.poll_messages(user_id, stream_id, topic_id)
    }

    pub fn store_consumer_offset(
        &self,
        user_id: u32,
        stream_id: u32,
        topic_id: u32,
    ) -> Result<(), IggyError> {
        self.poll_messages(user_id, stream_id, topic_id)
    }
}
