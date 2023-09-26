use iggy::client::UserClient;
use iggy::clients::client::IggyClient;
use iggy::users::login_user::LoginUser;
use iggy::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};

pub mod poll_messages_benchmark;
pub mod send_and_poll_messages_benchmark;
pub mod send_messages_benchmark;

pub(crate) async fn login_root(client: &IggyClient) {
    client
        .login_user(&LoginUser {
            username: DEFAULT_ROOT_USERNAME.to_string(),
            password: DEFAULT_ROOT_PASSWORD.to_string(),
        })
        .await
        .unwrap();
}
