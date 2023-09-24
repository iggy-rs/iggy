use crate::client::Client;
use anyhow::{Error, Result};
use async_trait::async_trait;

pub static PRINT_TARGET: &str = "iggy::cmd::output";

#[async_trait]
pub trait CliCommand {
    fn explain(&self) -> String;
    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error>;
}
