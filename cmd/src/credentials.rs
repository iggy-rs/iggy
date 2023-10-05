use crate::args::IggyConsoleArgs;
use crate::error::{CmdToolError, IggyCmdError};
use anyhow::Context;
use iggy::client::UserClient;
use iggy::clients::client::IggyClient;
use iggy::users::{login_user::LoginUser, logout_user::LogoutUser};
use passterm::{isatty, prompt_password_stdin, prompt_password_tty, Stream};
use std::env::var;

static ENV_IGGY_USERNAME: &str = "IGGY_USERNAME";
static ENV_IGGY_PASSWORD: &str = "IGGY_PASSWORD";

struct IggyUserClient {
    username: String,
    password: String,
}

pub(crate) struct IggyCredentials<'a> {
    credentials: Option<IggyUserClient>,
    iggy_client: Option<&'a IggyClient>,
    login_required: bool,
}

impl<'a> IggyCredentials<'a> {
    pub(crate) fn new(
        args: &IggyConsoleArgs,
        login_required: bool,
    ) -> anyhow::Result<Self, anyhow::Error> {
        if !login_required {
            return Ok(Self {
                credentials: None,
                iggy_client: None,
                login_required,
            });
        }

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
                credentials: Some(IggyUserClient {
                    username: username.clone(),
                    password,
                }),
                iggy_client: None,
                login_required,
            })
        } else if var(ENV_IGGY_USERNAME).is_ok() && var(ENV_IGGY_PASSWORD).is_ok() {
            Ok(Self {
                credentials: Some(IggyUserClient {
                    username: var(ENV_IGGY_USERNAME).unwrap(),
                    password: var(ENV_IGGY_PASSWORD).unwrap(),
                }),
                iggy_client: None,
                login_required,
            })
        } else {
            Err(IggyCmdError::CmdToolError(CmdToolError::MissingCredentials).into())
        }
    }

    pub(crate) fn set_iggy_client(&mut self, iggy_client: &'a IggyClient) {
        self.iggy_client = Some(iggy_client);
    }

    pub(crate) async fn login_user(&self) -> anyhow::Result<(), anyhow::Error> {
        if let Some(client) = self.iggy_client {
            if self.login_required {
                let _ = client
                    .login_user(&LoginUser {
                        username: self.credentials.as_ref().unwrap().username.clone(),
                        password: self.credentials.as_ref().unwrap().password.clone(),
                    })
                    .await
                    .with_context(|| {
                        format!(
                            "Problem with server login for username: {}",
                            self.credentials.as_ref().unwrap().username.clone()
                        )
                    })?;
            }
        }

        Ok(())
    }

    pub(crate) async fn logout_user(&self) -> anyhow::Result<(), anyhow::Error> {
        if let Some(client) = self.iggy_client {
            if self.login_required {
                client
                    .logout_user(&LogoutUser {})
                    .await
                    .with_context(|| "Problem with server logout".to_string())?;
            }
        }

        Ok(())
    }
}
