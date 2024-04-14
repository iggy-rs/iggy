use clap::Parser;
use figlet_rs::FIGfont;
use monoio::utils::CtrlC;
use server::args::Args;
use server::configs::config_provider;
use server::configs::server::ServerConfig;
use server::streaming::systems::system::{SharedSystem, System};
use std::error::Error;
use tracing::info;

#[monoio::main(timer_enabled = true)]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy TPC");
    println!("{}", figure.unwrap());

    let args = Args::parse();

    let config_provider = config_provider::resolve(&args.config_provider)?;
    let config = ServerConfig::load(config_provider.as_ref()).await?;

    let system = SharedSystem::new(System::new(
        config.system.clone(),
        None,
        config.personal_access_token,
    ));

    system.write().get_stats_bypass_auth().await?;
    system.write().init().await?;

    info!("Press CTRL+C shutdown Iggy TPC...");
    CtrlC::new().unwrap().await;
    info!("Iggy TPC has shutdown successfully.");

    Ok(())
}
