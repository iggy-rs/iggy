use clap::Parser;
use sdk::error::Error;
use tracing::error;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "http://localhost:3000")]
    pub http_api_url: String,

    #[arg(long, default_value = "1000000")]
    pub http_start_stream_id: u32,

    #[arg(long, default_value = "127.0.0.1:0")]
    pub quic_client_address: String,

    #[arg(long, default_value = "127.0.0.1:8080")]
    pub quic_server_address: String,

    #[arg(long, default_value = "localhost")]
    pub quic_server_name: String,

    #[arg(long, default_value = "2000000")]
    pub quic_start_stream_id: u32,

    #[arg(long, default_value = "0")]
    pub streams: u32,

    #[arg(long, default_value = "0")]
    pub producers: u32,

    #[arg(long, default_value = "0")]
    pub consumers: u32,

    #[arg(long, default_value = "false")]
    pub parallel_producer_streams: bool,

    #[arg(long, default_value = "false")]
    pub parallel_consumer_streams: bool,

    #[arg(long, default_value = "1000")]
    pub messages_per_batch: u32,

    #[arg(long, default_value = "1000")]
    pub message_batches: u32,

    #[arg(long, default_value = "1000")]
    pub message_size: u32,

    #[arg(long, default_value = "false")]
    pub http: bool,

    #[arg(long, default_value = "false")]
    pub quic: bool,

    #[arg(long, default_value = "false")]
    pub test_send_messages: bool,

    #[arg(long, default_value = "false")]
    pub test_poll_messages: bool,

    #[arg(long, default_value = "false")]
    pub test_send_and_poll_messages: bool,
}

impl Args {
    pub fn get_start_stream_id(&self) -> u32 {
        if self.http {
            self.http_start_stream_id
        } else {
            self.quic_start_stream_id
        }
    }

    pub fn validate(&self) -> Result<(), Error> {
        if !self.quic && !self.http {
            error!("At least one of the protocols must be provided: http or quic.");
            return Err(Error::InvalidConfiguration);
        }

        if self.streams == 0 {
            error!("The number of streams must be greater than zero.");
            return Err(Error::InvalidConfiguration);
        }

        if (self.test_send_messages || self.test_send_and_poll_messages) && self.producers == 0 {
            error!("The number of producers must be greater than zero.");
            return Err(Error::InvalidConfiguration);
        }

        if (self.test_poll_messages || self.test_send_and_poll_messages) && self.consumers == 0 {
            error!("The number of consumers must be greater than zero.");
            return Err(Error::InvalidConfiguration);
        }

        if self.parallel_consumer_streams && self.streams < self.consumers {
            error!(
                "The number of streams must be greater than or equal to the number of consumers."
            );
            return Err(Error::InvalidConfiguration);
        }

        if self.parallel_producer_streams && self.streams < self.producers {
            error!(
                "The number of streams must be greater than or equal to the number of producers."
            );
            return Err(Error::InvalidConfiguration);
        }

        Ok(())
    }
}
