use async_trait::async_trait;
use iggy::{error::IggyError, models::messages::Message};
use std::sync::Arc;

#[async_trait]
pub trait SegmentPersister {
    async fn append(
        &self,
        messages: Arc<Vec<Message>>,
        current_offset: u64,
    ) -> Result<(), IggyError>;
    async fn create(&self) -> Result<(), IggyError>;
    async fn delete(&self) -> Result<(), IggyError>;
}

#[derive(Debug)]
pub struct WithoutSync {
    pub path: String,
}

#[async_trait]
impl SegmentPersister for WithoutSync {
    async fn append(
        &self,
        messages: Arc<Vec<Message>>,
        current_offset: u64,
    ) -> Result<(), IggyError> {
        Ok(())
    }

    async fn create(&self) -> Result<(), IggyError> {
        println!("Creating without sync");
        Ok(())
    }

    async fn delete(&self) -> Result<(), IggyError> {
        println!("Deleting without sync");
        Ok(())
    }
}

#[derive(Debug)]
pub struct WithSync {
    pub path: String,
}

#[async_trait]
impl SegmentPersister for WithSync {
    async fn append(
        &self,
        messages: Arc<Vec<Message>>,
        current_position: u64,
    ) -> Result<(), IggyError> {
        println!("Appending with sync");
        Ok(())
    }

    async fn create(&self) -> Result<(), IggyError> {
        println!("Creating with sync");
        Ok(())
    }

    async fn delete(&self) -> Result<(), IggyError> {
        println!("Deleting with sync");
        Ok(())
    }
}

#[derive(Debug)]
pub struct StoragePersister<P: SegmentPersister> {
    persister: P,
}

impl<P: SegmentPersister> StoragePersister<P> {
    pub fn new(persister: P) -> Self {
        StoragePersister { persister }
    }

    pub async fn append(
        &self,
        messages: Arc<Vec<Message>>,
        current_offset: u64,
    ) -> Result<(), IggyError> {
        self.persister.append(messages, current_offset).await
    }

    pub async fn create(&self) -> Result<(), IggyError> {
        self.persister.create().await
    }

    pub async fn delete(&self) -> Result<(), IggyError> {
        self.persister.delete().await
    }
}
