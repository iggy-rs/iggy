use crate::args::CliOptions;
use crate::error::{CmdToolError, IggyCmdError};
use anyhow::{bail, Context};
use iggy::args::Args;
use iggy::cli::system::session::ServerSession;
use iggy::client::{PersonalAccessTokenClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::error::IggyError;
use passterm::{isatty, prompt_password_stdin, prompt_password_tty, Stream};
use std::env::var;

#[cfg(feature = "login-session")]
mod credentials_login_session {
    pub(crate) use iggy::cli_command::PRINT_TARGET;
    pub(crate) use keyring::Entry;
    pub(crate) use tracing::{event, Level};
}

#[cfg(feature = "login-session")]
use credentials_login_session::*;

static ENV_IGGY_USERNAME: &str = "IGGY_USERNAME";
static ENV_IGGY_PASSWORD: &str = "IGGY_PASSWORD";

struct IggyUserClient {
    username: String,
    password: String,
}

enum Credentials {
    UserNameAndPassword(IggyUserClient),
    PersonalAccessToken(String),
    SessionWithToken(String, String),
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

        if let Some(server_address) = iggy_args.get_server_address() {
            let server_session = ServerSession::new(server_address.clone());
            if let Some(token) = server_session.get_token() {
                return Ok(Self {
                    credentials: Some(Credentials::SessionWithToken(token, server_address)),
                    iggy_client: None,
                    login_required,
                });
            }
        }

        #[cfg(feature = "login-session")]
        if let Some(token_name) = &cli_options.token_name {
            return match iggy_args.get_server_address() {
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
            };
        }

        if let Some(token) = &cli_options.token {
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
                            .login_user(
                                &username_and_password.username,
                                &username_and_password.password,
                            )
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
                            .login_with_personal_access_token(token_value)
                            .await
                            .with_context(|| {
                                format!("Problem with server login with token: {}", token_value)
                            })?;
                    }
                    Credentials::SessionWithToken(token_value, server_address) => {
                        let login_result =
                            client.login_with_personal_access_token(token_value).await;
                        if let Err(err) = login_result {
                            match err {
                                IggyError::InvalidResponse(code, _, _) => {
                                    // Check what does the code means and if that's one of the cases
                                    // when we should delete the session and inform user to try login again
                                    // TODO: improve this part when we have more information about the codes
                                    if code == IggyError::ResourceNotFound(String::new()).as_code()
                                        || code == IggyError::Unauthenticated.as_code()
                                        || code
                                            == IggyError::PersonalAccessTokenExpired(
                                                String::new(),
                                                0,
                                            )
                                            .as_code()
                                    {
                                        let server_session =
                                            ServerSession::new(server_address.clone());
                                        server_session.delete()?;
                                        bail!("Login session expired for Iggy server: {server_address}, please login again or use other authentication method");
                                    }
                                }
                                _ => {
                                    bail!("Problem with server login with token: {token_value}");
                                }
                            }
                        }
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
                    .logout_user()
                    .await
                    .with_context(|| "Problem with server logout".to_string())?;
            }
        }

        Ok(())
    }
}
