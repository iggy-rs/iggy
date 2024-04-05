use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::models::permissions::Permissions;
use crate::users::update_permissions::UpdatePermissions;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct UpdatePermissionsCmd {
    update_permissions: UpdatePermissions,
}

impl UpdatePermissionsCmd {
    pub fn new(user_id: Identifier, permissions: Option<Permissions>) -> Self {
        Self {
            update_permissions: UpdatePermissions {
                user_id,
                permissions,
            },
        }
    }
}

#[async_trait]
impl CliCommand for UpdatePermissionsCmd {
    fn explain(&self) -> String {
        format!(
            "update permissions for user with ID: {}",
            self.update_permissions.user_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .update_permissions(
                &self.update_permissions.user_id,
                self.update_permissions.permissions.clone(),
            )
            .await
            .with_context(|| {
                format!(
                    "Problem updating permissions for user with ID: {}",
                    self.update_permissions.user_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Permissions for user with ID: {} updated",
            self.update_permissions.user_id
        );

        Ok(())
    }
}
