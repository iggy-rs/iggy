use crate::streaming::segments::segment::Segment;
use iggy::error::IggyError;

impl Segment {
    pub async fn load(&mut self) -> Result<(), IggyError> {
        let storage = self.storage.clone();
        storage.segment.load(self).await
    }

    pub async fn persist(&self) -> Result<(), IggyError> {
        self.storage.segment.save(self).await
    }
}
