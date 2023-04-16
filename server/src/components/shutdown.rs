use crate::server::Server;
use crate::server_command::ServerCommand;
use tracing::info;

impl Server {
    pub fn handle_shutdown(&self) {
        let name = self.config.name.clone();
        let sender = self.sender.clone();
        ctrlc::set_handler(move || {
            info!("Shutting down {} server...", name);
            let runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
            let shutdown = sender.send(ServerCommand::Shutdown);
            runtime.block_on(shutdown).unwrap();
        })
        .expect("Error setting Ctrl-C handler");
    }
}
