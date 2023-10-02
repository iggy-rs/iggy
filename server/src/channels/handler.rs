use super::executor::ExecutableServerCommand;
use crate::streaming::systems::system::System;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::warn;

pub struct ServerCommandHandler {
    system: Arc<RwLock<System>>,
}

impl ServerCommandHandler {
    pub fn new(system: Arc<RwLock<System>>) -> Self {
        Self { system }
    }

    pub fn install_handler<E>(&self, mut executor: E) -> flume::Sender<E::Command>
    where
        E: ExecutableServerCommand + Send + Sync + 'static,
    {
        let (sender, receiver) = flume::unbounded();

        let system = self.system.clone();
        tokio::spawn(async move {
            let system = system.clone();
            while let Ok(command) = receiver.recv_async().await {
                executor.execute(system.clone(), command).await
            }
            warn!("Server command handler stopped receiving commands.");
        });
        sender
    }
}
