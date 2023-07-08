mod common;
use crate::common::scenarios::{consumer_group_scenario, system_scenario};
use crate::common::tcp::TcpClientFactory;

#[tokio::test]
async fn system_scenario_should_be_valid() {
    let client_factory = TcpClientFactory {};
    system_scenario::run(&client_factory).await;
    consumer_group_scenario::run(&client_factory).await;
}
