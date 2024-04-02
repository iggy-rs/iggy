use crate::bench::run_bench_and_wait_for_finish;
use iggy::clients::next_client::IggyClientNext;
use iggy::clients::next_client::IggyClientNextBackgroundConfig;
use iggy::next_client::SystemClientNext;
use iggy::utils::byte_size::IggyByteSize;
use integration::{
    tcp_client::TcpClientFactory,
    test_server::{
        login_root_next, ClientFactoryNext, IpAddrKind, TestServer, Transport, SYSTEM_PATH_ENV_VAR,
    },
};
use serial_test::parallel;
use std::{collections::HashMap, str::FromStr};

#[tokio::test]
#[parallel]
async fn should_fill_data_and_verify_after_restart() {
    // 1. Start server
    let mut test_server = TestServer::new(None, false, None, IpAddrKind::V4);
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let local_data_path = test_server.get_local_data_path().to_owned();

    // 2. Run send bench to fill 5 MB of data
    let amount_of_data_to_process = IggyByteSize::from_str("5 MB").unwrap();
    run_bench_and_wait_for_finish(
        &server_addr,
        Transport::Tcp,
        "send",
        amount_of_data_to_process,
    );

    // 3. Run poll bench to check if everything's OK
    run_bench_and_wait_for_finish(
        &server_addr,
        Transport::Tcp,
        "poll",
        amount_of_data_to_process,
    );

    // 4. Connect and login to newly started server
    let client = TcpClientFactory { server_addr }.create_client().await;
    let client = IggyClientNext::create(
        client,
        IggyClientNextBackgroundConfig::default(),
        None,
        None,
        None,
    );
    login_root_next(&client).await;

    // 5. Save stats from the first server
    let stats = client.get_stats().await.unwrap();
    let expected_messages_size_bytes = stats.messages_size_bytes;
    let expected_streams_count = stats.streams_count;
    let expected_topics_count = stats.topics_count;
    let expected_partitions_count = stats.partitions_count;
    let expected_segments_count = stats.segments_count;
    let expected_messages_count = stats.messages_count;
    let expected_clients_count = stats.clients_count;
    let expected_consumer_groups_count = stats.consumer_groups_count;

    // 6. Stop server, remove current config file to properly fetch new server TCP address
    test_server.stop();
    drop(test_server);
    std::fs::remove_file(local_data_path.clone() + "/runtime/current_config.toml").unwrap();

    // 7. Restart server
    let extra_envs = HashMap::from([(SYSTEM_PATH_ENV_VAR.to_owned(), local_data_path.clone())]);
    let mut test_server = TestServer::new(Some(extra_envs), false, None, IpAddrKind::V4);
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();

    // 8. Connect and login to newly started server
    let client = IggyClientNext::create(
        TcpClientFactory {
            server_addr: server_addr.clone(),
        }
        .create_client()
        .await,
        IggyClientNextBackgroundConfig::default(),
        None,
        None,
        None,
    );
    login_root_next(&client).await;

    // 9. Save stats from the second server
    let stats = client.get_stats().await.unwrap();
    let actual_messages_size_bytes = stats.messages_size_bytes;
    let actual_streams_count = stats.streams_count;
    let actual_topics_count = stats.topics_count;
    let actual_partitions_count = stats.partitions_count;
    let actual_segments_count = stats.segments_count;
    let actual_messages_count = stats.messages_count;
    let actual_clients_count = stats.clients_count;
    let actual_consumer_groups_count = stats.consumer_groups_count;

    // 10. Compare stats
    assert_eq!(
        expected_messages_size_bytes, actual_messages_size_bytes,
        "Messages size bytes"
    );
    assert_eq!(
        expected_streams_count, actual_streams_count,
        "Streams count"
    );
    assert_eq!(expected_topics_count, actual_topics_count, "Topics count");
    assert_eq!(
        expected_partitions_count, actual_partitions_count,
        "Partitions count"
    );
    assert_eq!(
        expected_segments_count, actual_segments_count,
        "Segments count"
    );
    assert_eq!(
        expected_messages_count, actual_messages_count,
        "Messages count"
    );
    assert_eq!(
        expected_clients_count, actual_clients_count,
        "Clients count"
    );
    assert_eq!(
        expected_consumer_groups_count, actual_consumer_groups_count,
        "Consumer groups count"
    );

    // 11. Again run poll bench to check if data is still there
    run_bench_and_wait_for_finish(
        &server_addr,
        Transport::Tcp,
        "poll",
        amount_of_data_to_process,
    );

    // 12. Manual cleanup
    std::fs::remove_dir_all(local_data_path).unwrap();
}
