use figlet_rs::FIGfont;
use monoio::utils::CtrlC;
use std::error::Error;
use tracing::info;

#[monoio::main(timer_enabled = true)]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy TPC");
    println!("{}", figure.unwrap());
    info!("Press CTRL+C shutdown Iggy TPC...");
    CtrlC::new().unwrap().await;
    info!("Iggy TPC has shutdown successfully.");

    Ok(())
}
