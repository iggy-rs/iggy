use iggy::client::UserClient;
use iggy::clients::client::IggyClient;
use iggy::users::login_user::LoginUser;

pub mod poll_messages_benchmark;
pub mod send_and_poll_messages_benchmark;
pub mod send_messages_benchmark;

const ROOT_USERNAME: &str = "iggy";
const ROOT_PASSWORD: &str = "iggy";

pub(crate) async fn login_root(client: &IggyClient) {
    client
        .login_user(&LoginUser {
            username: ROOT_USERNAME.to_string(),
            password: ROOT_PASSWORD.to_string(),
        })
        .await
        .unwrap();
}
