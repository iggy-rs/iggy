use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::users::get_users::GetUsers;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use tracing::{event, Level};

pub enum GetUsersOutput {
    Table,
    List,
}

pub struct GetUsersCmd {
    _get_users: GetUsers,
    output: GetUsersOutput,
}

impl GetUsersCmd {
    pub fn new(output: GetUsersOutput) -> Self {
        GetUsersCmd {
            _get_users: GetUsers {},
            output,
        }
    }
}

impl Default for GetUsersCmd {
    fn default() -> Self {
        GetUsersCmd {
            _get_users: GetUsers {},
            output: GetUsersOutput::Table,
        }
    }
}

#[async_trait]
impl CliCommand for GetUsersCmd {
    fn explain(&self) -> String {
        let mode = match self.output {
            GetUsersOutput::Table => "table",
            GetUsersOutput::List => "list",
        };
        format!("list users in {mode} mode")
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let users = client
            .get_users()
            .await
            .with_context(|| String::from("Problem getting list of users"))?;

        if users.is_empty() {
            event!(target: PRINT_TARGET, Level::INFO, "No users found!");
            return Ok(());
        }

        match self.output {
            GetUsersOutput::Table => {
                let mut table = Table::new();

                table.set_header(vec!["ID", "Created", "Status", "Username"]);

                users.iter().for_each(|user| {
                    table.add_row(vec![
                        format!("{}", user.id),
                        user.created_at.to_local_string("%Y-%m-%d %H:%M:%S"),
                        user.status.clone().to_string(),
                        user.username.clone(),
                    ]);
                });

                event!(target: PRINT_TARGET, Level::INFO, "{table}");
            }
            GetUsersOutput::List => {
                users.iter().for_each(|user| {
                    event!(target: PRINT_TARGET, Level::INFO,
                        "{}|{}|{}|{}",
                        user.id,
                        user.created_at.to_local_string("%Y-%m-%d %H:%M:%S"),
                        user.status.clone().to_string(),
                        user.username.clone(),
                    );
                });
            }
        }

        Ok(())
    }
}
