use crate::streaming::streams::stream::Stream;
use iggy::error::IggyError;

impl Stream {
    pub async fn load(&mut self) -> Result<(), IggyError> {
        let storage = self.storage.clone();
        storage.stream.load(self).await
    }

    pub async fn persist(&self) -> Result<(), IggyError> {
        self.storage.stream.create(self).await
    }

    pub async fn delete(&self) -> Result<(), IggyError> {
        for topic in self.get_topics() {
            topic.delete().await?;
        }

        self.storage.stream.delete(self).await
    }

    pub async fn persist_messages(&self) -> Result<(), IggyError> {
        for topic in self.get_topics() {
            topic.persist_messages().await?;
        }

        Ok(())
    }

    pub async fn purge(&self) -> Result<(), IggyError> {
        for topic in self.get_topics() {
            topic.purge().await?;
        }
        Ok(())
    }
}
