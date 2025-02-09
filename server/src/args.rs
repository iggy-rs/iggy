use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, default_value = "file")]
    pub config_provider: String,

    #[arg(
        long,
        default_value_t = false,
        help = "Remove system path (local_data by default) before starting. THIS WILL REMOVE ALL SAVED DATA!"
    )]
    pub fresh: bool,
}
