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

    #[arg(long, default_value = "1048576")]
    pub response_buffer_size: u64,
}
