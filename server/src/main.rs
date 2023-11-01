mod args;
mod binary;
mod channels;
mod configs;
mod http;
mod logging;
mod quic;
mod server_error;
mod streaming;
mod tcp;

use crate::args::Args;
use crate::channels::commands::clean_messages::CleanMessagesExecutor;
use crate::channels::commands::clean_personal_access_tokens::CleanPersonalAccessTokensExecutor;
use crate::channels::commands::save_messages::SaveMessagesExecutor;
use crate::channels::handler::ServerCommandHandler;
use crate::configs::config_provider;
use crate::configs::server::ServerConfig;
use crate::http::http_server;
use crate::logging::Logging;
use crate::quic::quic_server;
use crate::server_error::ServerError;
use crate::streaming::persistence::persister::FileWithSyncPersister;
use crate::streaming::segments::storage::FileSegmentStorage;
use crate::streaming::systems::system::{SharedSystem, System};
use crate::tcp::tcp_server;
use anyhow::Result;
use clap::Parser;
use figlet_rs::FIGfont;
use std::sync::Arc;
use tokio::time::Instant;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), ServerError> {
    let startup_timestamp = Instant::now();
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy Server");
    println!("{}", figure.unwrap());

    let mut logging = Logging::new();
    logging.early_init();

    // From this point on, we can use tracing macros to log messages.

    let args = Args::parse();

    let config_provider = config_provider::resolve(&args.config_provider)?;
    let config = ServerConfig::load(config_provider.as_ref()).await?;

    logging.late_init(config.system.get_system_path(), &config.system.logging)?;

    let mut system = System::new(config.system.clone(), None, config.personal_access_token);

    system.init().await?;
    let system = SharedSystem::new(system);
    let _command_handler = ServerCommandHandler::new(system.clone(), &config)
        .install_handler(SaveMessagesExecutor)
        .install_handler(CleanMessagesExecutor)
        .install_handler(CleanPersonalAccessTokensExecutor);

    #[cfg(unix)]
    let (mut ctrl_c, mut sigterm) = {
        use tokio::signal::unix::{signal, SignalKind};
        (
            signal(SignalKind::interrupt())?,
            signal(SignalKind::terminate())?,
        )
    };

    #[cfg(windows)]
    let mut ctrl_c = tokio::signal::ctrl_c();

    if config.http.enabled {
        let system = system.clone();
        tokio::spawn(async move {
            http_server::start(config.http, system).await;
        });
    }

    if config.quic.enabled {
        quic_server::start(config.quic, system.clone());
    }

    if config.tcp.enabled {
        tcp_server::start(config.tcp, system.clone());
    }

    let elapsed_time = startup_timestamp.elapsed();
    info!(
        "Iggy server has started - overall startup took {} ms.",
        elapsed_time.as_millis()
    );
    #[cfg(unix)]
    tokio::select! {
        _ = ctrl_c.recv() => {
            info!("Received SIGINT. Shutting down Iggy server...");
        },
        _ = sigterm.recv() => {
            info!("Received SIGTERM. Shutting down Iggy server...");
        }
    }

    #[cfg(windows)]
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            info!("Received CTRL-C. Shutting down Iggy server...");
        }
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
        }
    }

    let shutdown_timestamp = Instant::now();
    let mut system = system.write().await;
    let persister = Arc::new(FileWithSyncPersister);
    let storage = Arc::new(FileSegmentStorage::new(persister));
    system.shutdown(storage).await?;
    let elapsed_time = shutdown_timestamp.elapsed();

    info!(
        "Iggy server has shutdown successfully. Shutdown took {} ms.",
        elapsed_time.as_millis()
    );
    Ok(())
}
