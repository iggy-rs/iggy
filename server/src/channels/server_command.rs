use crate::configs::server::ServerConfig;
use crate::streaming::systems::system::SharedSystem;
use async_trait::async_trait;
use flume::{Receiver, Sender};

#[async_trait]
pub trait ServerCommand<C> {
    async fn execute(&mut self, system: &SharedSystem, command: C);

    fn start_command_sender(
        &mut self,
        system: SharedSystem,
        config: &ServerConfig,
        sender: Sender<C>,
    );

    fn start_command_consumer(
        self,
        system: SharedSystem,
        config: &ServerConfig,
        receiver: Receiver<C>,
    );
}
