use anyhow::{Context, Error, Result};
use iggy::client::Client;
use iggy::users::{login_user::LoginUser, logout_user::LogoutUser};
use passterm::{isatty, prompt_password_stdin, prompt_password_tty, Stream};

pub(crate) async fn login_user(
    client: &dyn Client,
    username: String,
    password: String,
) -> anyhow::Result<(), anyhow::Error> {
    let user = username.clone();
    let _ = client
        .login_user(&LoginUser { username, password })
        .await
        .with_context(|| format!("Problem with server login for username: {}", user))?;

    Ok(())
}

pub(crate) async fn logout_user(client: &dyn Client) -> Result<(), Error> {
    client
        .logout_user(&LogoutUser {})
        .await
        .with_context(|| "Problem with server logout".to_string())?;

    Ok(())
}

pub(crate) fn get_password(cli_password: Option<String>) -> anyhow::Result<String> {
    let password = match cli_password {
        Some(password) => password,
        None => {
            if isatty(Stream::Stdin) {
                prompt_password_tty(Some("Password: "))?
            } else {
                prompt_password_stdin(None, Stream::Stdout)?
            }
        }
    };

    Ok(password)
}
