use crate::server::scenarios::{
    consumer_group_join_scenario, consumer_group_with_multiple_clients_polling_messages_scenario,
    consumer_group_with_single_client_polling_messages_scenario, create_message_payload, next,
    stream_size_validation_scenario, system_scenario, user_scenario,
};
use integration::{tcp_client::TcpClientFactory, test_server::TestServer};
use serial_test::parallel;

#[tokio::test]
#[parallel]
async fn system_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory { server_addr };
    system_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn system_scenario_next_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory { server_addr };
    next::system_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn user_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory { server_addr };
    user_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn user_scenario_next_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory { server_addr };
    next::user_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn message_headers_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory { server_addr };
    create_message_payload::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn message_headers_scenario_next_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory { server_addr };
    next::message_headers_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn consumer_group_join_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory { server_addr };
    consumer_group_join_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn consumer_group_join_scenario_next_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory { server_addr };
    next::consumer_group_join_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn consumer_group_with_single_client_polling_messages_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory { server_addr };
    consumer_group_with_single_client_polling_messages_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn consumer_group_with_single_client_polling_messages_scenario_next_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory { server_addr };
    next::consumer_group_with_single_client_polling_messages_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn consumer_group_with_multiple_clients_polling_messages_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory { server_addr };
    consumer_group_with_multiple_clients_polling_messages_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn consumer_group_with_multiple_clients_polling_messages_scenario_next_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory { server_addr };
    next::consumer_group_with_multiple_clients_polling_messages_scenario::run(&client_factory)
        .await;
}

#[tokio::test]
#[parallel]
async fn stream_size_validation_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory { server_addr };
    stream_size_validation_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn stream_size_validation_scenario_next_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory { server_addr };
    next::stream_size_validation_scenario::run(&client_factory).await;
}
