use crate::streaming::streams::stream::Stream;
use iggy::error::Error;

impl Stream {
    pub async fn load(&mut self) -> Result<(), Error> {
        let storage = self.storage.clone();
        storage.stream.load(self).await
    }

    pub async fn persist(&self) -> Result<(), Error> {
        self.storage.stream.save(self).await
    }

    pub async fn delete(&self) -> Result<(), Error> {
        for topic in self.get_topics() {
            topic.delete().await?;
        }

        self.storage.stream.delete(self).await
    }

    pub async fn persist_messages(&self) -> Result<(), Error> {
        for topic in self.get_topics() {
            topic.persist_messages().await?;
        }

        Ok(())
    }

    pub async fn purge(&self) -> Result<(), Error> {
        for topic in self.get_topics() {
            topic.purge().await?;
        }
        Ok(())
    }
}
