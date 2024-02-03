use std::{str::FromStr, sync::Arc};

use criterion::{criterion_group, criterion_main, Criterion};
use iggy::{
    client::{MessageClient, StreamClient, TopicClient},
    clients::client::{IggyClient, IggyClientConfig},
    identifier::Identifier,
    messages::send_messages::{Message, Partitioning, SendMessages},
    streams::{create_stream::CreateStream, get_streams::GetStreams},
    topics::create_topic::CreateTopic,
};
use integration::{
    tcp_client::TcpClientFactory,
    test_server::{login_root, ClientFactory, IpAddrKind, TestServer},
};
use tokio::runtime::Runtime;

fn create_payload(size: u32) -> String {
    let mut payload = String::with_capacity(size as usize);
    for i in 0..size {
        let char = (i % 26 + 97) as u8 as char;
        payload.push(char);
    }

    payload
}

fn setup(runtime: &Runtime) -> (TestServer, IggyClient) {
    runtime.block_on(async {
        let mut server = TestServer::new(None, false, None, IpAddrKind::V4);
        server.start();

        let server_addr = server.get_raw_tcp_addr().unwrap();

        let topic_id: u32 = 1;

        let client_factory = Arc::new(TcpClientFactory { server_addr })
            .create_client()
            .await;

        let client = IggyClient::create(
            client_factory,
            IggyClientConfig::default(),
            None,
            None,
            None,
        );
        login_root(&client).await;
        let number_of_streams = 1;
        let start_stream_id = 1;
        let partitions_count = 1;
        let streams = client.get_streams(&GetStreams {}).await.unwrap();
        for i in 0..number_of_streams {
            let stream_id = start_stream_id + i;
            if streams.iter().all(|s| s.id != stream_id) {
                let name = format!("stream {}", stream_id);
                client
                    .create_stream(&CreateStream {
                        stream_id: Some(stream_id),
                        name,
                    })
                    .await
                    .unwrap();

                let name = format!("topic {}", topic_id);
                client
                    .create_topic(&CreateTopic {
                        stream_id: Identifier::numeric(stream_id).unwrap(),
                        topic_id: Some(topic_id),
                        partitions_count,
                        name,
                        message_expiry: None,
                        max_topic_size: None,
                        replication_factor: 1,
                    })
                    .await
                    .unwrap();
            }
        }
        (server, client)
    })
}

fn teardown_server(_server: TestServer) {}

fn send_messages_benchmark(client: &IggyClient, runtime: &Runtime) -> usize {
    runtime.block_on(async {
        let payload = create_payload(1000);
        let messages_per_batch = 1000;
        let total_bytes_sent = payload.len() * messages_per_batch;

        let mut messages = Vec::with_capacity(messages_per_batch);
        for _ in 0..messages_per_batch {
            let message = Message::from_str(&payload).unwrap();
            messages.push(message);
        }

        let mut send_messages = SendMessages {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(1).unwrap(),
            partitioning: Partitioning::partition_id(1),
            messages,
        };

        client.send_messages(&mut send_messages).await.unwrap();
        total_bytes_sent
    })
}

fn tcp_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Server-Dependent-Benchmarks");
    let runtime = Runtime::new().unwrap();
    let (server, client) = setup(&runtime);

    group.throughput(criterion::Throughput::Bytes(
        send_messages_benchmark(&client, &runtime) as u64,
    ));

    group.bench_function("tcp_benchmark", |b| {
        b.iter(|| {
            send_messages_benchmark(&client, &runtime);
        });
    });

    group.finish();
    teardown_server(server);
}

criterion_group! {
    name = benches;
    config = Criterion::default()
    .sample_size(60)
    .warm_up_time(std::time::Duration::from_secs(5))
    .measurement_time(std::time::Duration::from_secs(10));
    targets = tcp_benchmark
}

criterion_main!(benches);
