use crate::server_tests::common::http::HttpClientFactory;
use crate::server_tests::common::scenarios::{
    message_headers_scenario, system_scenario, user_scenario,
};
use serial_test::serial;

#[tokio::test]
#[serial]
async fn system_scenario_should_be_valid() {
    let client_factory = HttpClientFactory {};
    system_scenario::run(&client_factory).await;
    user_scenario::run(&client_factory).await;
    message_headers_scenario::run(&client_factory).await;
}
