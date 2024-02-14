use crate::client::Client;
use anyhow::{Error, Result};
use async_trait::async_trait;

pub static PRINT_TARGET: &str = "iggy::cli::output";

#[async_trait]
pub trait CliCommand {
    fn explain(&self) -> String;
    fn use_tracing(&self) -> bool {
        true
    }
    fn login_required(&self) -> bool {
        true
    }
    fn connection_required(&self) -> bool {
        true
    }
    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error>;
}
