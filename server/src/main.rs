use clap::Parser;
use figlet_rs::FIGfont;
use monoio::time::Instant;
use monoio::utils::CtrlC;
use server::args::Args;
use server::channels::commands::clean_messages::CleanMessagesExecutor;
use server::channels::commands::clean_personal_access_tokens::CleanPersonalAccessTokensExecutor;
use server::channels::commands::print_sysinfo::SysInfoPrintExecutor;
use server::channels::commands::save_messages::SaveMessagesExecutor;
use server::channels::handler::ServerCommandHandler;
use server::configs::config_provider;
use server::configs::server::ServerConfig;
use server::log::logger::Logging;
use server::server_error::ServerError;
use server::streaming::systems::system::{SharedSystem, System};
use server::tcp::tcp_server;
use tracing::{error, info};

#[monoio::main(driver = "uring", timer_enabled = true)]
async fn main() -> Result<(), ServerError> {
    let startup_timestamp = Instant::now();
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy Server");
    println!("{}", figure.unwrap());

    let mut logging = Logging::new();
    logging.early_init();

    let args = Args::parse();

    let config_provider = config_provider::resolve(&args.config_provider)?;
    let config = ServerConfig::load(&config_provider).await?;

    logging.late_init(config.system.get_system_path(), &config.system.logging)?;

    let system = SharedSystem::new(System::new(
        config.system.clone(),
        None,
        config.personal_access_token,
    ));

    let _command_handler = ServerCommandHandler::new(system.clone(), &config)
        .install_handler(SaveMessagesExecutor)
        .install_handler(CleanMessagesExecutor)
        .install_handler(CleanPersonalAccessTokensExecutor)
        .install_handler(SysInfoPrintExecutor);

    system.write().get_stats_bypass_auth().await?;
    system.write().init().await?;

    // #[cfg(unix)]
    //     let (mut ctrl_c, mut sigterm) = {
    //     use monoio::signal::unix::{signal, SignalKind};
    //     (
    //         signal(SignalKind::interrupt())?,
    //         signal(SignalKind::terminate())?,
    //     )
    // };

    let mut current_config = config.clone();

    if config.tcp.enabled {
        let tcp_addr = tcp_server::start(config.tcp, system.clone()).await;
        current_config.tcp.address = tcp_addr.to_string();
    }

    let runtime_path = current_config.system.get_runtime_path();
    let current_config_path = format!("{}/current_config.toml", runtime_path);
    let current_config_content =
        toml::to_string(&current_config).expect("Cannot serialize current_config");
    if let Err(error) = monoio::fs::write(
        current_config_path,
        current_config_content.as_bytes().to_vec(),
    )
    .await
    .0
    {
        panic!("Cannot write current_config to file: {:?}", error);
    }

    let elapsed_time = startup_timestamp.elapsed();
    info!(
        "Iggy server has started - overall startup took {} ms.",
        elapsed_time.as_millis()
    );

    // #[cfg(unix)]
    // monoio::select! {
    //     _ = ctrl_c.recv() => {
    //         info!("Received SIGINT. Shutting down Iggy server...");
    //     },
    //     _ = sigterm.recv() => {
    //         info!("Received SIGTERM. Shutting down Iggy server...");
    //     }
    // }

    CtrlC::new().unwrap().await;
    let shutdown_timestamp = Instant::now();
    let mut system = system.write();
    system.shutdown().await?;
    let elapsed_time = shutdown_timestamp.elapsed();

    info!(
        "Iggy server has shutdown successfully. Shutdown took {} ms.",
        elapsed_time.as_millis()
    );

    Ok(())
}
