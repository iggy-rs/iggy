use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::system::get_clients::GetClients;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use tracing::{event, Level};

pub enum GetClientsOutput {
    Table,
    List,
}

pub struct GetClientsCmd {
    _get_clients: GetClients,
    output: GetClientsOutput,
}

impl GetClientsCmd {
    pub fn new(output: GetClientsOutput) -> Self {
        GetClientsCmd {
            _get_clients: GetClients {},
            output,
        }
    }
}

impl Default for GetClientsCmd {
    fn default() -> Self {
        GetClientsCmd {
            _get_clients: GetClients {},
            output: GetClientsOutput::Table,
        }
    }
}

#[async_trait]
impl CliCommand for GetClientsCmd {
    fn explain(&self) -> String {
        let mode = match self.output {
            GetClientsOutput::Table => "table",
            GetClientsOutput::List => "list",
        };
        format!("list clients in {mode} mode")
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let clients = client
            .get_clients()
            .await
            .with_context(|| String::from("Problem getting list of clients"))?;

        if clients.is_empty() {
            event!(target: PRINT_TARGET, Level::INFO, "No clients found!");
            return Ok(());
        }

        match self.output {
            GetClientsOutput::Table => {
                let mut table = Table::new();

                table.set_header(vec![
                    "Client ID",
                    "User ID",
                    "Address",
                    "Transport",
                    "Consumer Groups",
                ]);

                clients.iter().for_each(|client_info| {
                    table.add_row(vec![
                        format!("{}", client_info.client_id),
                        match client_info.user_id {
                            Some(user_id) => format!("{}", user_id),
                            None => String::from(""),
                        },
                        format!("{}", client_info.address),
                        format!("{}", client_info.transport),
                        format!("{}", client_info.consumer_groups_count),
                    ]);
                });

                event!(target: PRINT_TARGET, Level::INFO, "{table}");
            }
            GetClientsOutput::List => {
                clients.iter().for_each(|client_info| {
                    event!(target: PRINT_TARGET, Level::INFO,
                        "{}|{}|{}|{}|{}",
                        client_info.client_id,
                        match client_info.user_id {
                            Some(user_id) => format!("{}", user_id),
                            None => String::from(""),
                        },
                        client_info.address,
                        client_info.transport,
                        client_info.consumer_groups_count
                    );
                });
            }
        }

        Ok(())
    }
}
