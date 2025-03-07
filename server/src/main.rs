use anyhow::Result;
use clap::Parser;
use dotenvy::dotenv;
use figlet_rs::FIGfont;
use server::args::Args;
use server::channels::commands::archive_state::ArchiveStateExecutor;
use server::channels::commands::clean_personal_access_tokens::CleanPersonalAccessTokensExecutor;
use server::channels::commands::maintain_messages::MaintainMessagesExecutor;
use server::channels::commands::print_sysinfo::SysInfoPrintExecutor;
use server::channels::commands::save_messages::SaveMessagesExecutor;
use server::channels::commands::verify_heartbeats::VerifyHeartbeatsExecutor;
use server::channels::handler::BackgroundServerCommandHandler;
use server::configs::config_provider;
use server::configs::server::ServerConfig;
use server::http::http_server;
#[cfg(not(feature = "tokio-console"))]
use server::log::logger::Logging;
#[cfg(feature = "tokio-console")]
use server::log::tokio_console::Logging;
use server::quic::quic_server;
use server::server_error::ServerError;
use server::streaming::systems::system::{SharedSystem, System};
use server::tcp::tcp_server;
use tokio::time::Instant;
use tracing::{info, instrument};

#[tokio::main]
#[instrument(skip_all, name = "trace_start_server")]
async fn main() -> Result<(), ServerError> {
    let startup_timestamp = Instant::now();
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy Server");
    println!("{}", figure.unwrap());

    if let Ok(env_path) = std::env::var("IGGY_ENV_PATH") {
        if dotenvy::from_path(&env_path).is_ok() {
            println!("Loaded environment variables from path: {env_path}");
        }
    } else if let Ok(path) = dotenv() {
        println!(
            "Loaded environment variables from .env file at path: {}",
            path.display()
        );
    }

    let args = Args::parse();
    let config_provider = config_provider::resolve(&args.config_provider)?;
    let config = ServerConfig::load(&config_provider).await?;
    if args.fresh {
        let system_path = config.system.get_system_path();
        if tokio::fs::metadata(&system_path).await.is_ok() {
            println!(
                "Removing system path at: {} because `--fresh` flag was set",
                system_path
            );
            if let Err(e) = tokio::fs::remove_dir_all(&system_path).await {
                eprintln!("Failed to remove system path at {}: {}", system_path, e);
            }
        }
    }
    let mut logging = Logging::new(config.telemetry.clone());
    logging.early_init();

    // From this point on, we can use tracing macros to log messages.

    logging.late_init(config.system.get_system_path(), &config.system.logging)?;

    #[cfg(feature = "disable-mimalloc")]
    tracing::warn!(
        "Using default system allocator because code was build with `disable-mimalloc` feature"
    );
    #[cfg(not(feature = "disable-mimalloc"))]
    info!("Using mimalloc allocator");

    let system = SharedSystem::new(System::new(
        config.system.clone(),
        config.data_maintenance.clone(),
        config.personal_access_token.clone(),
    ));

    // Workaround to ensure that the statistics are initialized before the server
    // loads streams and starts accepting connections. This is necessary to
    // have the correct statistics when the server starts.
    system.write().await.get_stats().await?;
    system.write().await.init().await?;

    let _command_handler = BackgroundServerCommandHandler::new(system.clone(), &config)
        .install_handler(SaveMessagesExecutor)
        .install_handler(MaintainMessagesExecutor)
        .install_handler(ArchiveStateExecutor)
        .install_handler(CleanPersonalAccessTokensExecutor)
        .install_handler(SysInfoPrintExecutor)
        .install_handler(VerifyHeartbeatsExecutor);

    #[cfg(unix)]
    let (mut ctrl_c, mut sigterm) = {
        use tokio::signal::unix::{signal, SignalKind};
        (
            signal(SignalKind::interrupt())?,
            signal(SignalKind::terminate())?,
        )
    };

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
    let mut system = system.write().await;
    system.shutdown().await?;
    let elapsed_time = shutdown_timestamp.elapsed();

    info!(
        "Iggy server has shutdown successfully. Shutdown took {} ms.",
        elapsed_time.as_millis()
    );
    Ok(())
}
