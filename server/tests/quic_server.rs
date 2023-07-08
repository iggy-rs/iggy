mod common;
use crate::common::quic::QuicClientFactory;
use crate::common::scenarios::{consumer_group_scenario, system_scenario};

#[tokio::test]
async fn system_scenario_should_be_valid() {
    let client_factory = QuicClientFactory {};
    system_scenario::run(&client_factory).await;
    consumer_group_scenario::run(&client_factory).await;
}
