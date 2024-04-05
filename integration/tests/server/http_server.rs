use crate::server::scenarios::{
    create_message_payload, stream_size_validation_scenario, system_scenario, user_scenario,
};
use integration::{http_client::HttpClientFactory, test_server::TestServer};
use serial_test::parallel;

#[tokio::test]
#[parallel]
async fn create_message_payload_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_http_api_addr().unwrap();
    let client_factory = HttpClientFactory { server_addr };
    create_message_payload::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn message_headers_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_http_api_addr().unwrap();
    let client_factory = HttpClientFactory { server_addr };
    create_message_payload::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn stream_size_validation_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_http_api_addr().unwrap();
    let client_factory = HttpClientFactory { server_addr };
    stream_size_validation_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn system_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_http_api_addr().unwrap();
    let client_factory = HttpClientFactory { server_addr };
    system_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn user_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_http_api_addr().unwrap();
    let client_factory = HttpClientFactory { server_addr };
    user_scenario::run(&client_factory).await;
}
