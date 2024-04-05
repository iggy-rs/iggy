use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::system::get_me::GetMe;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use tracing::{event, Level};

pub struct GetMeCmd {
    _get_me: GetMe,
}

impl GetMeCmd {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for GetMeCmd {
    fn default() -> Self {
        Self { _get_me: GetMe {} }
    }
}

#[async_trait]
impl CliCommand for GetMeCmd {
    fn explain(&self) -> String {
        "me command".to_owned()
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let client_info = client
            .get_me()
            .await
            .with_context(|| "Problem sending get_me command".to_owned())?;

        let mut table = Table::new();

        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec![
            "Client ID",
            format!("{}", client_info.client_id).as_str(),
        ]);
        if let Some(user_id) = client_info.user_id {
            table.add_row(vec!["User ID", format!("{}", user_id).as_str()]);
        }
        table.add_row(vec!["Address", client_info.address.as_str()]);
        table.add_row(vec!["Transport", client_info.transport.as_str()]);

        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}
