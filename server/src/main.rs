use anyhow::Result;
use clap::Parser;
use figlet_rs::FIGfont;
use server::args::Args;
use server::channels::commands::clean_messages::CleanMessagesExecutor;
use server::channels::commands::clean_personal_access_tokens::CleanPersonalAccessTokensExecutor;
use server::channels::commands::save_messages::SaveMessagesExecutor;
use server::channels::handler::ServerCommandHandler;
use server::configs::config_provider;
use server::configs::server::ServerConfig;
use server::http::http_server;
use server::logging::Logging;
use server::quic::quic_server;
use server::server_error::ServerError;
use server::streaming::persistence::persister::FileWithSyncPersister;
use server::streaming::segments::storage::FileSegmentStorage;
use server::streaming::systems::system::{SharedSystem, System};
use server::tcp::tcp_server;
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

    let mut current_config = config.clone();

    if config.http.enabled {
        let http_addr = http_server::start(config.http, system.clone()).await;
        current_config.http.address = http_addr.to_string();
    }

    if config.quic.enabled {
        let quic_addr = quic_server::start(config.quic, system.clone());
        current_config.quic.address = quic_addr.to_string();
    }

    if config.tcp.enabled {
        let tcp_addr = tcp_server::start(config.tcp, system.clone()).await;
        current_config.tcp.address = tcp_addr.to_string();
    }

    let runtime_path = current_config.system.get_runtime_path();
    let current_config_path = format!("{}/current_config.toml", runtime_path);
    let current_config_content =
        toml::to_string(&current_config).expect("Cannot serialize current_config");

    tokio::fs::write(current_config_path, current_config_content).await?;

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
    let mut system = system.write();
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
