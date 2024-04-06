use crate::server::scenarios::{
    consumer_group_join_scenario, consumer_group_with_multiple_clients_polling_messages_scenario,
    consumer_group_with_single_client_polling_messages_scenario, create_message_payload,
    message_headers_scenario, stream_size_validation_scenario, system_scenario, user_scenario,
};
use integration::{quic_client::QuicClientFactory, test_server::TestServer};
use serial_test::parallel;

#[tokio::test]
#[parallel]
async fn system_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    system_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn user_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    user_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn message_headers_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    message_headers_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn create_message_payload_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    create_message_payload::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn consumer_group_join_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    consumer_group_join_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn consumer_group_with_single_client_polling_messages_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    consumer_group_with_single_client_polling_messages_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn consumer_group_with_multiple_clients_polling_messages_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    consumer_group_with_multiple_clients_polling_messages_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn stream_size_validation_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    stream_size_validation_scenario::run(&client_factory).await;
}
