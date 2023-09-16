use async_trait::async_trait;
use iggy::client::Client;

#[async_trait]
pub(crate) trait CliCommand {
    fn explain(&self) -> String;
    async fn execute_cmd(&mut self, client: &dyn Client);
}
