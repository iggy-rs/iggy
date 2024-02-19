use crate::cli::common::IggyCmdTest;
use crate::cli::system::test_login_cmd::TestLoginCmd;
use crate::cli::system::test_logout_cmd::TestLogoutCmd;
use crate::cli::system::test_me_command::TestMeCmd;
use serial_test::serial;

#[tokio::test]
#[serial]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    let server_address = iggy_cmd_test.get_sever_ip_address();
    assert!(server_address.is_some());
    let server_address = server_address.unwrap();

    iggy_cmd_test
        .execute_test(TestLoginCmd::new(server_address.clone()))
        .await;
    iggy_cmd_test.execute_test(TestMeCmd::default()).await;
    iggy_cmd_test
        .execute_test(TestLogoutCmd::new(server_address))
        .await;
}
