use crate::state::system::StreamState;
use crate::streaming::storage::StreamStorage;
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

    pub async fn delete(&self, remove_from_disk: bool) -> Result<(), IggyError> {
        for topic in self.get_topics() {
            topic.delete(remove_from_disk).await?;
        }

        if remove_from_disk {
            return self.storage.stream.delete(self).await;
        }
        Ok(())
    }

    pub async fn persist_messages(&self) -> Result<usize, IggyError> {
        let mut saved_messages_number = 0;
        for topic in self.get_topics() {
            saved_messages_number += topic.persist_messages().await?;
        }

        Ok(saved_messages_number)
    }

    pub async fn purge(&self, purge_on_disk: bool) -> Result<(), IggyError> {
        for topic in self.get_topics() {
            topic.purge(purge_on_disk).await?;
        }
        Ok(())
    }
}
