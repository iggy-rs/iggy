use crate::{configs::server::ServerConfig, streaming::systems::system::System};
use async_trait::async_trait;
use flume::{Receiver, Sender};
use std::sync::Arc;
use tokio::sync::RwLock;

#[async_trait]
pub trait ServerCommand<C> {
    async fn execute(&mut self, system: &Arc<RwLock<System>>, command: C);

    fn start_command_sender(
        &mut self,
        system: Arc<RwLock<System>>,
        config: &ServerConfig,
        sender: Sender<C>,
    );

    fn start_command_consumer(
        self,
        system: Arc<RwLock<System>>,
        config: &ServerConfig,
        receiver: Receiver<C>,
    );
}
