use crate::args::CliOptions;
use crate::error::{CmdToolError, IggyCmdError};
use anyhow::Context;
use iggy::args::Args;
use iggy::cli_command::PRINT_TARGET;
use iggy::client::{PersonalAccessTokenClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy::users::{login_user::LoginUser, logout_user::LogoutUser};
use keyring::Entry;
use passterm::{isatty, prompt_password_stdin, prompt_password_tty, Stream};
use std::env::var;
use tracing::{event, Level};

static ENV_IGGY_USERNAME: &str = "IGGY_USERNAME";
static ENV_IGGY_PASSWORD: &str = "IGGY_PASSWORD";

struct IggyUserClient {
    username: String,
    password: String,
}

enum Credentials {
    UserNameAndPassword(IggyUserClient),
    PersonalAccessToken(String),
}

pub(crate) struct IggyCredentials<'a> {
    credentials: Option<Credentials>,
    iggy_client: Option<&'a IggyClient>,
    login_required: bool,
}

impl<'a> IggyCredentials<'a> {
    pub(crate) fn new(
        cli_options: &CliOptions,
        iggy_args: &Args,
        login_required: bool,
    ) -> anyhow::Result<Self, anyhow::Error> {
        if !login_required {
            return Ok(Self {
                credentials: None,
                iggy_client: None,
                login_required,
            });
        }

        if let Some(token_name) = &cli_options.token_name {
            match iggy_args.get_server_address() {
                Some(server_address) => {
                    let server_address = format!("iggy:{}", server_address);
                    event!(target: PRINT_TARGET, Level::DEBUG,"Checking token presence under service: {} and name: {}",
                    server_address, token_name);
                    let entry = Entry::new(&server_address, token_name)?;
                    let token = entry.get_password()?;

                    Ok(Self {
                        credentials: Some(Credentials::PersonalAccessToken(token)),
                        iggy_client: None,
                        login_required,
                    })
                }
                None => Err(IggyCmdError::CmdToolError(CmdToolError::MissingServerAddress).into()),
            }
        } else if let Some(token) = &cli_options.token {
            Ok(Self {
                credentials: Some(Credentials::PersonalAccessToken(token.clone())),
                iggy_client: None,
                login_required,
            })
        } else if let Some(username) = &cli_options.username {
            let password = match &cli_options.password {
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
                credentials: Some(Credentials::UserNameAndPassword(IggyUserClient {
                    username: username.clone(),
                    password,
                })),
                iggy_client: None,
                login_required,
            })
        } else if var(ENV_IGGY_USERNAME).is_ok() && var(ENV_IGGY_PASSWORD).is_ok() {
            Ok(Self {
                credentials: Some(Credentials::UserNameAndPassword(IggyUserClient {
                    username: var(ENV_IGGY_USERNAME).unwrap(),
                    password: var(ENV_IGGY_PASSWORD).unwrap(),
                })),
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
                let credentials = self.credentials.as_ref().unwrap();
                match credentials {
                    Credentials::UserNameAndPassword(username_and_password) => {
                        let _ = client
                            .login_user(&LoginUser {
                                username: username_and_password.username.clone(),
                                password: username_and_password.password.clone(),
                            })
                            .await
                            .with_context(|| {
                                format!(
                                    "Problem with server login for username: {}",
                                    &username_and_password.username
                                )
                            })?;
                    }
                    Credentials::PersonalAccessToken(token_value) => {
                        let _ = client
                            .login_with_personal_access_token(&LoginWithPersonalAccessToken {
                                token: token_value.clone(),
                            })
                            .await
                            .with_context(|| {
                                format!("Problem with server login with token: {}", &token_value)
                            })?;
                    }
                }
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
