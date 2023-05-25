use crate::server_command::ServerCommand;
use flume::Sender;
use std::process;
use tracing::{error, info};

pub fn handle(sender: Sender<ServerCommand>) {
    ctrlc::set_handler(move || {
        info!("Shutting down Iggy server...");
        let runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
        let shutdown = sender.send_async(ServerCommand::Shutdown);
        let block_on = runtime.block_on(shutdown);
        if let Err(error) = block_on {
            error!("Error when shutting down Iggy server: {}", error);
            process::exit(1);
        }
    })
    .expect("Error setting Ctrl-C handler");
}
