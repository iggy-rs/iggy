use crate::state::system::StreamState;
use crate::streaming::streams::stream::Stream;
use iggy::error::IggyError;

impl Stream {
    pub async fn load(&mut self, state: StreamState) -> Result<(), IggyError> {
        let storage = self.storage.clone();
        storage.stream.load(self, state).await
    }

    pub async fn persist(&self) -> Result<(), IggyError> {
        self.storage.stream.save(self).await
    }

    pub async fn delete(&self) -> Result<(), IggyError> {
        for topic in self.get_topics() {
            topic.delete().await?;
        }

        self.storage.stream.delete(self).await
    }

    pub async fn persist_messages(&self) -> Result<usize, IggyError> {
        let mut saved_messages_number = 0;
        for topic in self.get_topics() {
            saved_messages_number += topic.persist_messages().await?;
        }

        Ok(saved_messages_number)
    }

    pub async fn purge(&self) -> Result<(), IggyError> {
        for topic in self.get_topics() {
            topic.purge().await?;
        }
        Ok(())
    }
}
