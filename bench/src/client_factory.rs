use crate::args::Args;
use async_trait::async_trait;
use sdk::client::Client;
use std::sync::Arc;

#[async_trait]
pub trait ClientFactory: Sync + Send {
    async fn create_client(&self, args: Arc<Args>) -> Box<dyn Client>;
}
