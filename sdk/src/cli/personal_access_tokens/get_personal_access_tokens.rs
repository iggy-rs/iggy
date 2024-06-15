use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use tracing::{event, Level};

pub enum GetPersonalAccessTokensOutput {
    Table,
    List,
}

pub struct GetPersonalAccessTokensCmd {
    _get_tokens: GetPersonalAccessTokens,
    output: GetPersonalAccessTokensOutput,
}

impl GetPersonalAccessTokensCmd {
    pub fn new(output: GetPersonalAccessTokensOutput) -> Self {
        Self {
            _get_tokens: GetPersonalAccessTokens {},
            output,
        }
    }
}

#[async_trait]
impl CliCommand for GetPersonalAccessTokensCmd {
    fn explain(&self) -> String {
        let mode = match self.output {
            GetPersonalAccessTokensOutput::Table => "table",
            GetPersonalAccessTokensOutput::List => "list",
        };
        format!("list personal access tokens in {mode} mode")
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let tokens = client
            .get_personal_access_tokens()
            .await
            .with_context(|| String::from("Problem getting list of personal access tokens"))?;

        match self.output {
            GetPersonalAccessTokensOutput::Table => {
                let mut table = Table::new();

                table.set_header(vec!["Name", "Token Expiry Time"]);

                tokens.iter().for_each(|token| {
                    table.add_row(vec![
                        format!("{}", token.name.clone()),
                        match token.expiry_at {
                            None => String::from("unlimited"),
                            Some(value) => value.to_local_string("%Y-%m-%d %H:%M:%S"),
                        },
                    ]);
                });

                event!(target: PRINT_TARGET, Level::INFO, "{table}");
            }
            GetPersonalAccessTokensOutput::List => {
                tokens.iter().for_each(|token| {
                    event!(target: PRINT_TARGET, Level::INFO,
                        "{}|{}",
                        token.name,
                        match token.expiry_at {
                            None => String::from("unlimited"),
                            Some(value) => value.to_local_string("%Y-%m-%d %H:%M:%S"),
                        },
                    );
                });
            }
        }

        Ok(())
    }
}
