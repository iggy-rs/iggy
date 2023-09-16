use crate::cli::CliCommand;
use async_trait::async_trait;
use comfy_table::Table;
use iggy::client::Client;
use iggy::streams::get_streams::GetStreams;

#[derive(Debug, Default)]
pub(crate) struct StreamBrowse {}

#[async_trait]
impl CliCommand for StreamBrowse {
    fn explain(&self) -> String {
        String::from("browse list of streams")
    }

    fn needs_tui(&self) -> bool {
        true
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) {}

    async fn execute_tui(&mut self, client: Option<&dyn Client>) {}
}
