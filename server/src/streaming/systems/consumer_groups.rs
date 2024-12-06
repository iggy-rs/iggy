use crate::streaming::session::Session;
use crate::streaming::systems::COMPONENT;
use crate::streaming::systems::system::System;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use error_set::ResultContext;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::locking::IggySharedMutFn;
use tokio::sync::RwLock;

impl System {
    pub fn get_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<&RwLock<ConsumerGroup>, IggyError> {
        self.ensure_authenticated(session)?;
        let topic = self.find_topic(session, stream_id, topic_id)
            .with_error(|_| format!("{COMPONENT} - topic not found for stream_id: {stream_id}, topic_id: {topic_id}"))?;

        self.permissioner
            .get_consumer_group(session.get_user_id(), topic.stream_id, topic.topic_id)
            .with_error(|_| {
                format!(
                    "{COMPONENT} - permission denied to get consumer group for user {} on stream_id: {}, topic_id: {}",
                    session.get_user_id(),
                    topic.stream_id,
                    topic.topic_id
                )
            })?;

        topic.get_consumer_group(group_id).with_error(|_| {
            format!("{COMPONENT} - consumer group not found for group_id: {group_id}")
        })
    }

    pub fn get_consumer_groups(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<&RwLock<ConsumerGroup>>, IggyError> {
        self.ensure_authenticated(session)?;
        let topic = self.find_topic(session, stream_id, topic_id)
            .with_error(|_| format!("{COMPONENT} - topic not found for stream_id: {stream_id}, topic_id: {topic_id}"))?;

        self.permissioner
            .get_consumer_groups(session.get_user_id(), topic.stream_id, topic.topic_id)
            .with_error(|_| {
                format!(
                    "{COMPONENT} - permission denied to get consumer groups for user {} on stream_id: {}, topic_id: {}",
                    session.get_user_id(),
                    topic.stream_id,
                    topic.topic_id
                )
            })?;

        Ok(topic.get_consumer_groups())
    }

    pub async fn create_consumer_group(
        &mut self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: Option<u32>,
        name: &str,
    ) -> Result<&RwLock<ConsumerGroup>, IggyError> {
        self.ensure_authenticated(session)?;
        {
            let topic = self.find_topic(session, stream_id, topic_id)
                .with_error(|_| format!("{COMPONENT} - topic not found for stream_id: {stream_id}, topic_id: {topic_id}"))?;

            self.permissioner.create_consumer_group(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error(|_| format!("{COMPONENT} - permission denied to create consumer group for user {} on stream_id: {}, topic_id: {}", session.get_user_id(), topic.stream_id, topic.topic_id))?;
        }

        let topic = self.get_stream_mut(stream_id)?
            .get_topic_mut(topic_id)
            .with_error(|_| format!("{COMPONENT} - topic not found for stream_id: {stream_id}, topic_id: {topic_id}"))?;

        topic
            .create_consumer_group(group_id, name)
            .await
            .with_error(|_| {
                format!("{COMPONENT} - failed to create consumer group with name: {name}")
            })
    }

    pub async fn delete_consumer_group(
        &mut self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let stream_id_value;
        let topic_id_value;
        {
            let topic = self.find_topic(session, stream_id, topic_id)
                .with_error(|_| format!("{COMPONENT} - topic not found for stream_id: {stream_id}, topic_id: {topic_id}"))?;

            self.permissioner.delete_consumer_group(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error(|_| format!("{COMPONENT} - permission denied to delete consumer group for user {} on stream_id: {}, topic_id: {}", session.get_user_id(), topic.stream_id, topic.topic_id))?;

            stream_id_value = topic.stream_id;
            topic_id_value = topic.topic_id;
        }

        let consumer_group;
        {
            let stream = self.get_stream_mut(stream_id).with_error(|_| {
                format!(
                    "{COMPONENT} - failed to get mutable reference to stream with id: {stream_id}"
                )
            })?;
            let topic = stream.get_topic_mut(topic_id).with_error(|_| format!("{COMPONENT} - topic not found for stream_id: {stream_id}, topic_id: {topic_id}"))?;

            consumer_group = topic.delete_consumer_group(consumer_group_id)
                .await
                .with_error(|_| format!("{COMPONENT} - failed to delete consumer group for consumer_group_id: {consumer_group_id}"))?;
        }

        let client_manager = self.client_manager.read().await;
        let consumer_group = consumer_group.read().await;
        for member in consumer_group.get_members() {
            let member = member.read().await;
            client_manager
                .leave_consumer_group(
                    member.id,
                    stream_id_value,
                    topic_id_value,
                    consumer_group.group_id,
                )
                .await
                .with_error(|_| format!("{COMPONENT} - failed to make client leave consumer group for client_id: {}, group_id: {}", member.id, consumer_group.group_id))?;
        }

        Ok(())
    }

    pub async fn join_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let stream_id_value;
        let topic_id_value;
        {
            let topic = self
                .find_topic(session, stream_id, topic_id)
                .with_error(|_| {
                    format!(
                        "{COMPONENT} - topic not found for stream_id: {stream_id}, topic_id: {topic_id}",
                    )
                })?;

            self.permissioner.join_consumer_group(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error(|_| format!("{COMPONENT} - permission denied to join consumer group for user {} on stream_id: {}, topic_id: {}", session.get_user_id(), topic.stream_id, topic.topic_id))?;

            stream_id_value = topic.stream_id;
            topic_id_value = topic.topic_id;
        }

        let group_id;
        {
            let topic = self.find_topic(session, stream_id, topic_id)?;

            {
                let consumer_group =
                    topic
                        .get_consumer_group(consumer_group_id)
                        .with_error(|_| {
                            format!(
                                "{COMPONENT} - consumer group not found for group_id: {:?}",
                                consumer_group_id
                            )
                        })?;

                let consumer_group = consumer_group.read().await;
                group_id = consumer_group.group_id;
            }

            topic
                .join_consumer_group(consumer_group_id, session.client_id)
                .await
                .with_error(|_| {
                    format!(
                        "{COMPONENT} - failed to join consumer group for group_id: {}",
                        group_id
                    )
                })?;
        }

        let client_manager = self.client_manager.read().await;
        client_manager
            .join_consumer_group(session.client_id, stream_id_value, topic_id_value, group_id)
            .await
            .with_error(|_| {
                format!(
                    "{COMPONENT} - failed to make client join consumer group for client_id: {}",
                    session.client_id
                )
            })?;

        Ok(())
    }

    pub async fn leave_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        {
            let topic = self
                .find_topic(session, stream_id, topic_id)
                .with_error(|_| {
                    format!(
                        "{COMPONENT} - topic not found for stream_id: {:?}, topic_id: {:?}",
                        stream_id, topic_id
                    )
                })?;

            self.permissioner.leave_consumer_group(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error(|_| format!("{COMPONENT} - permission denied to leave consumer group for user {} on stream_id: {}, topic_id: {}", session.get_user_id(), topic.stream_id, topic.topic_id))?;
        }

        self.leave_consumer_group_by_client(
            stream_id,
            topic_id,
            consumer_group_id,
            session.client_id,
        )
        .await
        .with_error(|_| {
            format!(
                "{COMPONENT} - failed to leave consumer group for client_id: {}",
                session.client_id
            )
        })
    }

    pub async fn leave_consumer_group_by_client(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
        client_id: u32,
    ) -> Result<(), IggyError> {
        let stream_id_value;
        let topic_id_value;
        let group_id;

        {
            let stream = self.get_stream(stream_id).with_error(|_| {
                format!("{COMPONENT} - failed to get stream with id: {stream_id}")
            })?;
            let topic = stream.get_topic(topic_id)
                .with_error(|_| {
                    format!(
                        "{COMPONENT} - topic not found for stream_id: {stream_id}, topic_id: {topic_id}",
                    )
                })?;
            {
                let consumer_group =
                    topic
                        .get_consumer_group(consumer_group_id)
                        .with_error(|_| {
                            format!(
                        "{COMPONENT} - consumer group not found for group_id: {consumer_group_id}",
                    )
                        })?;
                let consumer_group = consumer_group.read().await;
                group_id = consumer_group.group_id;
            }

            stream_id_value = stream.stream_id;
            topic_id_value = topic.topic_id;
            topic
                .leave_consumer_group(consumer_group_id, client_id)
                .await
                .with_error(|_| {
                    format!("{COMPONENT} - failed leave consumer group, client ID {client_id}",)
                })?;
        }

        let client_manager = self.client_manager.read().await;
        client_manager
            .leave_consumer_group(client_id, stream_id_value, topic_id_value, group_id)
            .await
    }
}
