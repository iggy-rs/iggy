use crate::server::Server;
use crate::server_command::ServerCommand;
use std::process;
use tracing::{error, info};

impl Server {
    pub fn handle_shutdown(&self) {
        let name = self.config.name.clone();
        let sender = self.sender.clone();
        ctrlc::set_handler(move || {
            info!("Shutting down {} server...", name);
            let runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
            let shutdown = sender.send_async(ServerCommand::Shutdown);
            let block_on = runtime.block_on(shutdown);
            if let Err(error) = block_on {
                error!("Error when shutting down {}: {}", name, error);
                process::exit(1);
            }
        })
        .expect("Error setting Ctrl-C handler");
    }
}
