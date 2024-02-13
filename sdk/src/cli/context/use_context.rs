use async_trait::async_trait;
use tracing::{event, Level};

use crate::{
    cli_command::{CliCommand, PRINT_TARGET},
    client::Client,
};

use super::common::{ContextManager, DEFAULT_CONTEXT_NAME};

pub struct UseContextCmd {
    context_name: String,
}

impl UseContextCmd {
    pub fn new(context_name: String) -> Self {
        Self { context_name }
    }
}

impl Default for UseContextCmd {
    fn default() -> Self {
        UseContextCmd {
            context_name: DEFAULT_CONTEXT_NAME.to_string(),
        }
    }
}

#[async_trait]
impl CliCommand for UseContextCmd {
    fn explain(&self) -> String {
        let context_name = &self.context_name;
        format!("use context {context_name}")
    }

    fn login_required(&self) -> bool {
        false
    }

    fn connection_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut context_mgr = ContextManager::default();

        context_mgr
            .set_active_context_key(&self.context_name)
            .await?;

        event!(target: PRINT_TARGET, Level::INFO, "active context set to '{}'", self.context_name);

        return Ok(());
    }
}
