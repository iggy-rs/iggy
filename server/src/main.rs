use std::panic;
use std::thread::available_parallelism;

use clap::Parser;
use figlet_rs::FIGfont;
use iggy::error::IggyError;
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
use server::tpc::bootstrap::{create_shard, shard_executor};
use server::tpc::connector::ShardConnector;
use server::tpc::shard_frame::ShardFrame;
use tracing::{error, info};

fn main() -> Result<(), ServerError> {
    let available_cpus = available_parallelism().unwrap().into();
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

    let cpu_set = 0..available_cpus;
    let connections = cpu_set
        .clone()
        .into_iter()
        .map(|cpu| {
            let cpu = cpu as u16;
            let connector: ShardConnector<ShardFrame> = ShardConnector::new(cpu, available_cpus);
            connector
        })
        .collect::<Vec<_>>();

    let handles = cpu_set
        .into_iter()
        .map(|cpu| {
            let cpu = cpu as u16;
            std::thread::spawn(move || {
                monoio::utils::bind_to_cpu_set(Some(cpu as usize)).expect("Cannot bind to CPU set");
                let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                    .enable_timer()
                    .with_blocking_strategy(monoio::blocking::BlockingStrategy::ExecuteLocal)
                    .build()
                    .expect("Cannot build runtime");

                rt.block_on(async move {
                    let connections = connections.clone();
                    let shard = create_shard(cpu, connections);
                    if let Err(e) = shard_executor(shard, cpu == 0).await {
                        error!("Failed to start shard executor with error: {}", e);
                    }
                })
            })
        })
        .collect::<Vec<_>>();

    handles
    .into_iter()
    .map(|handle| match handle.join() {
        Err(e) => panic::resume_unwind(e),
        _ => {}
    }).collect::<Vec<_>>();

    /*
    let system = SharedSystem::new(System::new(
        config.system.clone(),
        None,
        config.personal_access_token,
    ));
    */

    // For now no bg handlers.
    /*
    let _command_handler = ServerCommandHandler::new(system.clone(), &config)
        .install_handler(SaveMessagesExecutor)
        .install_handler(CleanMessagesExecutor)
        .install_handler(CleanPersonalAccessTokensExecutor)
        .install_handler(SysInfoPrintExecutor);
    */

    /* 
    system.write().get_stats_bypass_auth().await?;
    system.write().init().await?;
    */

    // #[cfg(unix)]
    //     let (mut ctrl_c, mut sigterm) = {
    //     use monoio::signal::unix::{signal, SignalKind};
    //     (
    //         signal(SignalKind::interrupt())?,
    //         signal(SignalKind::terminate())?,
    //     )
    // };

    /*
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
    */

    Ok(())
}
