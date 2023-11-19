use crate::cmd::common::{
    IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, TestUserId, CLAP_INDENT,
    USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::models::user_info::UserId;
use iggy::users::create_user::CreateUser;
use iggy::users::delete_user::DeleteUser;
use iggy::users::get_user::GetUser;
use iggy::{client::Client, identifier::Identifier};
use predicates::str::diff;
use serial_test::parallel;

struct TestUserPasswordCmd {
    username: String,
    password: String,
    new_password: String,
    using_identifier: TestUserId,
    user_id: Option<UserId>,
}

impl TestUserPasswordCmd {
    fn new(
        username: String,
        password: String,
        new_password: String,
        using_identifier: TestUserId,
    ) -> Self {
        Self {
            username,
            password,
            new_password,
            using_identifier,
            user_id: None,
        }
    }

    fn to_args(&self) -> Vec<String> {
        match self.using_identifier {
            TestUserId::Named => vec![
                self.username.clone(),
                self.password.clone(),
                format!("{}", self.new_password.clone()),
            ],
            TestUserId::Numeric => {
                vec![
                    format!("{}", self.user_id.unwrap()),
                    self.password.clone(),
                    format!("{}", self.new_password.clone()),
                ]
            }
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestUserPasswordCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let create_user = client
            .create_user(&CreateUser {
                username: self.username.clone(),
                password: self.password.clone(),
                ..Default::default()
            })
            .await;
        assert!(create_user.is_ok());
        let user = client
            .get_user(&GetUser {
                user_id: Identifier::from_str_value(self.username.as_str()).unwrap(),
            })
            .await;
        assert!(user.is_ok());
        self.user_id = Some(user.unwrap().id);
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("user")
            .arg("password")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let message = match self.using_identifier {
            TestUserId::Named => format!(
                "Executing change password for user with ID: {}\nPassword for user with ID: {} changed\n",
                self.username, self.username
            ),
            TestUserId::Numeric => format!(
                "Executing change password for user with ID: {}\nPassword for user with ID: {} changed\n",
                self.user_id.unwrap(), self.user_id.unwrap()
            ),
        };

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let deleted = client
            .delete_user(&DeleteUser {
                user_id: Identifier::numeric(self.user_id.unwrap()).unwrap(),
            })
            .await;
        assert!(deleted.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestUserPasswordCmd::new(
            String::from("user"),
            String::from("password"),
            String::from("new-password"),
            TestUserId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserPasswordCmd::new(
            String::from("admin"),
            String::from("easy-pass"),
            String::from("P@$$w0r4"),
            TestUserId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserPasswordCmd::new(
            String::from("easy_one"),
            String::from("1234"),
            String::from("4321"),
            TestUserId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserPasswordCmd::new(
            String::from("same_all_the_time"),
            String::from("password"),
            String::from("password"),
            TestUserId::Named,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["user", "password", "--help"],
            format!(
                r#"Change password for user with given ID

User ID can be specified as a username or ID

Examples:
 iggy user password 2
 iggy user password client
 iggy user password 3 current_password new_password
 iggy user password testuser curpwd p@sswor4

{USAGE_PREFIX} user password <USER_ID> [CURRENT_PASSWORD] [NEW_PASSWORD]

Arguments:
  <USER_ID>
          User ID to update
{CLAP_INDENT}
          User ID can be specified as a username or ID

  [CURRENT_PASSWORD]
          Current password
{CLAP_INDENT}
          Current password, must be between 3 and 100 characters long.
          An optional parameter to specify the current password for the given user.
          If not provided, the user will be prompted interactively to enter the
          password securely, and the quiet mode option will not have any effect.

  [NEW_PASSWORD]
          New password
{CLAP_INDENT}
          New password, must be between 3 and 100 characters long.
          An optional parameter to specify the new password for the given user.
          If not provided, the user will be prompted interactively to enter the
          password securely, and the quiet mode option will not have any effect.

Options:
  -h, --help
          Print help (see a summary with '-h')
"#,
            ),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_short_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["user", "password", "-h"],
            format!(
                r#"Change password for user with given ID

{USAGE_PREFIX} user password <USER_ID> [CURRENT_PASSWORD] [NEW_PASSWORD]

Arguments:
  <USER_ID>           User ID to update
  [CURRENT_PASSWORD]  Current password
  [NEW_PASSWORD]      New password

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
