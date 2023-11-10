use crate::cmd::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestUserId};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::models::user_status::UserStatus;
use iggy::users::create_user::CreateUser;
use iggy::users::get_users::GetUsers;
use predicates::str::diff;
use serial_test::parallel;

struct TestUserLoginCmd {}

impl TestUserLoginCmd {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl IggyCmdTestCase for TestUserLoginCmd {
    async fn prepare_server_state(&self, client: &dyn Client) {
        let user = client
            .create_user(&CreateUser {
                username: self.username.clone(),
                password: self.password.clone(),
                status: self.status,
                permissions: None,
            })
            .await;
        assert!(user.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("user")
            .arg("delete")
            .arg(self.to_arg())
            .with_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let message = match self.using_identifier {
            TestUserId::Named => format!(
                "Executing delete user with ID: {}\nUser with ID: {} deleted\n",
                self.username, self.username
            ),
            TestUserId::Numeric => format!(
                "Executing delete user with ID: {}\nUser with ID: {} deleted\n",
                self.user_id, self.user_id
            ),
        };

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let users = client.get_users(&GetUsers {}).await;
        assert!(users.is_ok());
        let users = users.unwrap();
        assert_eq!(users.len(), 1); // Only root
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestUserLoginCmd::new(
            String::from("username"),
            String::from("password"),
            UserStatus::Active,
            2,
            TestUserId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserLoginCmd::new(
            String::from("testuser"),
            String::from("testpass"),
            UserStatus::Inactive,
            2,
            TestUserId::Named,
        ))
        .await;
}
