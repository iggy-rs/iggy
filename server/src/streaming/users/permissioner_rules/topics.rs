use crate::streaming::users::permissioner::Permissioner;
use iggy::error::IggyError;

impl Permissioner {
    pub fn get_topic(&self, user_id: u32, stream_id: u32, topic_id: u32) -> Result<(), IggyError> {
        if let Some(global_permissions) = self.users_permissions.get(&user_id) {
            if global_permissions.read_streams
                || global_permissions.manage_streams
                || global_permissions.manage_topics
                || global_permissions.read_topics
            {
                return Ok(());
            }
        }

        if let Some(stream_permissions) = self.users_streams_permissions.get(&(user_id, stream_id))
        {
            if stream_permissions.manage_topics || stream_permissions.read_topics {
                return Ok(());
            }

            if let Some(topic_permissions) =
                stream_permissions.topics.as_ref().unwrap().get(&topic_id)
            {
                if topic_permissions.manage_topic || topic_permissions.read_topic {
                    return Ok(());
                }
            }
        }

        Err(IggyError::Unauthorized)
    }

    pub fn get_topics(&self, user_id: u32, stream_id: u32) -> Result<(), IggyError> {
        if let Some(global_permissions) = self.users_permissions.get(&user_id) {
            if global_permissions.read_streams
                || global_permissions.manage_streams
                || global_permissions.manage_topics
                || global_permissions.read_topics
            {
                return Ok(());
            }
        }

        if let Some(stream_permissions) = self.users_streams_permissions.get(&(user_id, stream_id))
        {
            if stream_permissions.manage_topics || stream_permissions.read_topics {
                return Ok(());
            }

            if let Some(topic_permissions) =
                stream_permissions.topics.as_ref().unwrap().get(&stream_id)
            {
                if topic_permissions.manage_topic || topic_permissions.read_topic {
                    return Ok(());
                }
            }
        }

        Err(IggyError::Unauthorized)
    }

    pub fn create_topic(&self, user_id: u32, stream_id: u32) -> Result<(), IggyError> {
        if let Some(global_permissions) = self.users_permissions.get(&user_id) {
            if global_permissions.manage_streams || global_permissions.manage_topics {
                return Ok(());
            }
        }

        if let Some(stream_permissions) = self.users_streams_permissions.get(&(user_id, stream_id))
        {
            if stream_permissions.manage_topics {
                return Ok(());
            }
        }

        Err(IggyError::Unauthorized)
    }

    pub fn update_topic(
        &self,
        user_id: u32,
        stream_id: u32,
        topic_id: u32,
    ) -> Result<(), IggyError> {
        self.manage_topic(user_id, stream_id, topic_id)
    }

    pub fn delete_topic(
        &self,
        user_id: u32,
        stream_id: u32,
        topic_id: u32,
    ) -> Result<(), IggyError> {
        self.manage_topic(user_id, stream_id, topic_id)
    }

    pub fn purge_topic(
        &self,
        user_id: u32,
        stream_id: u32,
        topic_id: u32,
    ) -> Result<(), IggyError> {
        self.manage_topic(user_id, stream_id, topic_id)
    }

    fn manage_topic(&self, user_id: u32, stream_id: u32, topic_id: u32) -> Result<(), IggyError> {
        if let Some(global_permissions) = self.users_permissions.get(&user_id) {
            if global_permissions.manage_streams || global_permissions.manage_topics {
                return Ok(());
            }
        }

        if let Some(stream_permissions) = self.users_streams_permissions.get(&(user_id, stream_id))
        {
            if stream_permissions.manage_topics {
                return Ok(());
            }

            if let Some(topic_permissions) =
                stream_permissions.topics.as_ref().unwrap().get(&topic_id)
            {
                if topic_permissions.manage_topic {
                    return Ok(());
                }
            }
        }

        Err(IggyError::Unauthorized)
    }
}
