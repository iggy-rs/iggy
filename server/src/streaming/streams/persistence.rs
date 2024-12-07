use crate::state::system::StreamState;
use crate::streaming::streams::COMPONENT;
use crate::streaming::streams::stream::Stream;
use error_set::ResultContext;
use iggy::error::IggyError;

impl Stream {
    pub async fn load(&mut self, state: StreamState) -> Result<(), IggyError> {
        let storage = self.storage.clone();
        let state_id = state.id;
        storage.stream
            .load(self, state)
            .await
            .with_error(|_| format!("{COMPONENT} - failed to load stream with state, state ID: {state_id}, stream: {self}"))
    }

    pub async fn persist(&self) -> Result<(), IggyError> {
        self.storage.stream
            .save(self)
            .await
            .with_error(|_| format!("{COMPONENT} - failed to persist stream: {self}"))
    }

    pub async fn delete(&self) -> Result<(), IggyError> {
        for topic in self.get_topics() {
            topic
                .delete()
                .await
                .with_error(|_| format!("{COMPONENT} - failed to delete topic in stream: {self}"))?;
        }

        self.storage.stream
            .delete(self)
            .await
            .with_error(|_| format!("{COMPONENT} - failed to delete stream: {self}"))
    }

    pub async fn persist_messages(&self) -> Result<usize, IggyError> {
        let mut saved_messages_number = 0;
        for topic in self.get_topics() {
            saved_messages_number += topic
                .persist_messages()
                .await
                .with_error(|_| format!("{COMPONENT} - failed to persist messages for topic: {topic} in stream: {self}"))?;
        }

        Ok(saved_messages_number)
    }

    pub async fn purge(&self) -> Result<(), IggyError> {
        for topic in self.get_topics() {
            topic
                .purge()
                .await
                .with_error(|err| format!("{COMPONENT} - failed to purge topic: {topic} in stream: {self}"))?;
        }
        Ok(())
    }
}
