use crate::systems::system::System;
use crate::users::permissions::{GlobalPermissions, Permissions};
use crate::users::user::{Status, User};
use iggy::error::Error;
use iggy::utils::timestamp;
use tracing::info;

impl System {
    pub(crate) async fn load_users(&mut self) -> Result<(), Error> {
        info!("Loading users...");
        let mut users = self.storage.user.load_all().await?;
        if users.is_empty() {
            info!("No users found, creating the default user...");
            // TODO: Create the root user method, secure the password
            let root = User {
                id: 1,
                status: Status::Active,
                username: "iggy".to_string(),
                password: "iggy".to_string(),
                created_at: timestamp::get(),
                permissions: Some(Permissions {
                    global: GlobalPermissions {
                        manage_servers: true,
                        manage_users: true,
                        manage_streams: true,
                        manage_topics: true,
                        read_streams: true,
                        poll_messages: true,
                        send_messages: true,
                    },
                    streams: None,
                }),
            };
            self.storage.user.save(&root).await?;
            info!("Created the default user.");
            users = self.storage.user.load_all().await?;
        }

        let users_count = users.len();
        self.permissions_validator.init(users);
        info!("Initialized {} user(s).", users_count);
        Ok(())
    }
}
