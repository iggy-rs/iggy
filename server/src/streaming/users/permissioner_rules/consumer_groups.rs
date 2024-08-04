use crate::streaming::users::permissioner::Permissioner;
use iggy::error::IggyError;

impl Permissioner {
    pub fn create_consumer_group(
        &self,
        user_id: u32,
        stream_id: u32,
        topic_id: u32,
    ) -> Result<(), IggyError> {
        self.get_topic(user_id, stream_id, topic_id)
    }

    pub fn delete_consumer_group(
        &self,
        user_id: u32,
        stream_id: u32,
        topic_id: u32,
    ) -> Result<(), IggyError> {
        self.get_topic(user_id, stream_id, topic_id)
    }

    pub fn get_consumer_group(
        &self,
        user_id: u32,
        stream_id: u32,
        topic_id: u32,
    ) -> Result<(), IggyError> {
        self.get_topic(user_id, stream_id, topic_id)
    }

    pub fn get_consumer_groups(
        &self,
        user_id: u32,
        stream_id: u32,
        topic_id: u32,
    ) -> Result<(), IggyError> {
        self.get_topic(user_id, stream_id, topic_id)
    }

    pub fn join_consumer_group(
        &self,
        user_id: u32,
        stream_id: u32,
        topic_id: u32,
    ) -> Result<(), IggyError> {
        self.get_topic(user_id, stream_id, topic_id)
    }

    pub fn leave_consumer_group(
        &self,
        user_id: u32,
        stream_id: u32,
        topic_id: u32,
    ) -> Result<(), IggyError> {
        self.get_topic(user_id, stream_id, topic_id)
    }
}
