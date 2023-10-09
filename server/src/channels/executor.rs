use crate::streaming::systems::system::System;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

#[async_trait]
pub trait ExecutableServerCommand {
    type Command: Send + Sync + 'static;

    async fn execute(&mut self, system: Arc<RwLock<System>>, command: Self::Command);
}
