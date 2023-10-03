use crate::args::IggyConsoleArgs;
use crate::error::{CmdToolError, IggyCmdError};
use anyhow::Context;
use iggy::client::Client;
use iggy::users::{login_user::LoginUser, logout_user::LogoutUser};
use passterm::{isatty, prompt_password_stdin, prompt_password_tty, Stream};
use std::env::var;

static ENV_IGGY_USERNAME: &str = "IGGY_USERNAME";
static ENV_IGGY_PASSWORD: &str = "IGGY_PASSWORD";

pub(crate) struct IggyCredentials {
    username: String,
    password: String,
}

impl IggyCredentials {
    pub(crate) fn new(args: &IggyConsoleArgs) -> anyhow::Result<Self, anyhow::Error> {
        if let Some(username) = &args.username {
            let password = match &args.password {
                Some(password) => password.clone(),
                None => {
                    if isatty(Stream::Stdin) {
                        prompt_password_tty(Some("Password: "))?
                    } else {
                        prompt_password_stdin(None, Stream::Stdout)?
                    }
                }
            };

            Ok(Self {
                username: username.clone(),
                password,
            })
        } else if var(ENV_IGGY_USERNAME).is_ok() && var(ENV_IGGY_PASSWORD).is_ok() {
            Ok(Self {
                username: var(ENV_IGGY_USERNAME).unwrap(),
                password: var(ENV_IGGY_PASSWORD).unwrap(),
            })
        } else {
            Err(IggyCmdError::CmdToolError(CmdToolError::MissingCredentials).into())
        }
    }

    pub(crate) async fn login_user(
        &self,
        client: &dyn Client,
    ) -> anyhow::Result<(), anyhow::Error> {
        // let user = self.username.clone();
        let _ = client
            .login_user(&LoginUser {
                username: self.username.clone(),
                password: self.password.clone(),
            })
            .await
            .with_context(|| {
                format!(
                    "Problem with server login for username: {}",
                    self.username.clone()
                )
            })?;

        Ok(())
    }

    pub(crate) async fn logout_user(
        &self,
        client: &dyn Client,
    ) -> anyhow::Result<(), anyhow::Error> {
        client
            .logout_user(&LogoutUser {})
            .await
            .with_context(|| "Problem with server logout".to_string())?;

        Ok(())
    }
}
