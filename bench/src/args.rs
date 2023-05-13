use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "127.0.0.1:0")]
    pub client_address: String,

    #[arg(long, default_value = "127.0.0.1:8080")]
    pub server_address: String,

    #[arg(long, default_value = "localhost")]
    pub server_name: String,

    #[arg(long, default_value = "1000")]
    pub messages_per_batch: u32,

    #[arg(long, default_value = "1000")]
    pub message_batches: u32,

    #[arg(long, default_value = "1")]
    pub connections_count: u32,
}
