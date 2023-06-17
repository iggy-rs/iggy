use crate::segments::segment::Segment;
use sdk::error::Error;

impl Segment {
    pub async fn load(&mut self) -> Result<(), Error> {
        let storage = self.storage.clone();
        storage.segment.load(self).await
    }

    pub async fn persist(&self) -> Result<(), Error> {
        self.storage.segment.save(self).await
    }
}
