use clap::Parser;
use futures_util::future::join_all;
use futures_util::StreamExt;
use iggy::client::{AutoSignIn, Client, Credentials, StreamClient, TopicClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::clients::consumer::{AutoCommit, AutoCommitMode, IggyConsumer};
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::models::permissions::{Permissions, StreamPermissions, TopicPermissions};
use iggy::models::user_status::UserStatus;
use iggy::tcp::client::TcpClient;
use iggy::users::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use iggy::utils::duration::IggyDuration;
use iggy_examples::shared::args::Args;
use std::collections::HashMap;
use std::error::Error;
use std::str::FromStr;
use tokio::task::JoinHandle;
use tracing::{error, info};

const TENANT1_STREAM: &str = "tenant_1";
const TENANT2_STREAM: &str = "tenant_2";
const TENANT3_STREAM: &str = "tenant_3";
const TENANT1_USER: &str = "tenant_1_consumer";
const TENANT2_USER: &str = "tenant_2_consumer";
const TENANT3_USER: &str = "tenant_3_consumer";
const PASSWORD: &str = "secret";
const TOPIC: &str = "logs";
const CONSUMER_GROUP: &str = "multi-tenant-consumer";

struct TenantConsumer {
    tenant: String,
    consumer: IggyConsumer,
}

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    print_info("Multi-tenant consumer has started");
    let address = args.tcp_server_address;

    print_info("Creating root client to manage streams and users");
    let root_client = create_client(&address, DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD).await?;

    print_info("Creating users with permissions for each tenant");
    create_user(TENANT1_STREAM, TOPIC, TENANT1_USER, &root_client).await?;
    create_user(TENANT2_STREAM, TOPIC, TENANT2_USER, &root_client).await?;
    create_user(TENANT3_STREAM, TOPIC, TENANT3_USER, &root_client).await?;

    print_info("Disconnecting root client");
    root_client.disconnect().await?;

    print_info("Creating clients for each tenant");
    let tenant1_client = create_client(&address, TENANT1_USER, PASSWORD).await?;
    let tenant2_client = create_client(&address, TENANT2_USER, PASSWORD).await?;
    let tenant3_client = create_client(&address, TENANT3_USER, PASSWORD).await?;

    print_info("Ensuring access to topics for each tenant");
    ensure_topic_access(
        &tenant1_client,
        TOPIC,
        TENANT1_STREAM,
        &[TENANT2_STREAM, TENANT3_STREAM],
    )
    .await?;
    ensure_topic_access(
        &tenant2_client,
        TOPIC,
        TENANT2_STREAM,
        &[TENANT1_STREAM, TENANT3_STREAM],
    )
    .await?;
    ensure_topic_access(
        &tenant3_client,
        TOPIC,
        TENANT3_STREAM,
        &[TENANT1_STREAM, TENANT2_STREAM],
    )
    .await?;

    print_info("Creating consumer for each tenant");
    let consumer1 = create_consumer("tenant_1", &tenant1_client, TENANT1_STREAM, TOPIC).await?;
    let consumer2 = create_consumer("tenant_2", &tenant2_client, TENANT2_STREAM, TOPIC).await?;
    let consumer3 = create_consumer("tenant_3", &tenant3_client, TENANT3_STREAM, TOPIC).await?;

    print_info("Starting consumers for each tenant");
    let consumer1_task = start_consumer(consumer1);
    let consumer2_task = start_consumer(consumer2);
    let consumer3_task = start_consumer(consumer3);

    let tasks = vec![consumer1_task, consumer2_task, consumer3_task];
    join_all(tasks).await;

    print_info("Disconnecting clients");

    Ok(())
}

async fn create_user(
    stream_name: &str,
    topic_name: &str,
    username: &str,
    client: &IggyClient,
) -> Result<(), IggyError> {
    let stream = client.get_stream(&stream_name.try_into()?).await?;
    let topic = client
        .get_topic(&stream_name.try_into()?, &topic_name.try_into()?)
        .await?;
    let mut topic_permissions = HashMap::new();
    topic_permissions.insert(
        topic.id,
        TopicPermissions {
            read_topic: true,
            poll_messages: true,
            ..Default::default()
        },
    );

    let mut streams_permissions = HashMap::new();
    streams_permissions.insert(
        stream.id,
        StreamPermissions {
            read_stream: true,
            topics: Some(topic_permissions),
            ..Default::default()
        },
    );
    let permissions = Permissions {
        streams: Some(streams_permissions),
        ..Default::default()
    };
    let user = client
        .create_user(username, PASSWORD, UserStatus::Active, Some(permissions))
        .await?;
    info!(
        "Created user: {username} with ID: {}, with permissions for topic: {topic_name} in stream: {stream_name}",
        user.id
    );
    Ok(())
}

fn start_consumer(mut consumer: TenantConsumer) -> JoinHandle<()> {
    tokio::spawn(async move {
        let tenant = consumer.tenant;
        while let Some(message) = consumer.consumer.next().await {
            if let Ok(message) = message {
                let current_offset = message.current_offset;
                let partition_id = message.partition_id;
                let offset = message.message.offset;
                let payload =
                    std::str::from_utf8(&message.message.payload).expect("Invalid payload");
                info!("Tenant: {tenant} consumer received: {payload} from partition: {partition_id}, at offset: {offset}, current offset: {current_offset}");
            } else if let Err(error) = message {
                error!("Error while handling message: {error}, by: {tenant} consumer.");
                continue;
            }
        }
    })
}

async fn create_consumer(
    tenant: &str,
    client: &IggyClient,
    stream: &str,
    topic: &str,
) -> Result<TenantConsumer, IggyError> {
    let mut consumer = client
        .consumer_group(CONSUMER_GROUP, stream, topic)?
        .batch_size(10)
        .poll_interval(IggyDuration::from_str("10ms").expect("Invalid duration"))
        .polling_strategy(PollingStrategy::next())
        .auto_join_consumer_group()
        .auto_commit(AutoCommit::Mode(AutoCommitMode::AfterPollingMessages))
        .build();
    consumer.init().await?;
    Ok(TenantConsumer {
        tenant: tenant.to_owned(),
        consumer,
    })
}

async fn ensure_topic_access(
    client: &IggyClient,
    topic: &str,
    available_stream: &str,
    unavailable_streams: &[&str],
) -> Result<(), IggyError> {
    let topic_id = Identifier::named(topic)?;
    client
        .get_topic(&available_stream.try_into()?, &topic_id)
        .await
        .unwrap_or_else(|_| panic!("No access to topic: {topic} in stream: {available_stream}"));
    info!("Ensured access to topic: {topic} in stream: {available_stream}");
    for stream in unavailable_streams {
        if client
            .get_topic(&Identifier::named(stream)?, &topic_id)
            .await
            .is_err()
        {
            info!("Ensured no access to topic: {topic} in stream: {stream}");
        } else {
            panic!("Access to topic: {topic} in stream: {stream} should not be allowed");
        }
    }
    Ok(())
}

async fn create_client(
    address: &str,
    username: &str,
    password: &str,
) -> Result<IggyClient, IggyError> {
    let tcp_client = TcpClient::new(
        address,
        AutoSignIn::Enabled(Credentials::UsernamePassword(
            username.to_owned(),
            password.to_owned(),
        )),
    )?;
    let client = IggyClient::builder()
        .with_client(Box::new(tcp_client))
        .build()?;
    client.connect().await?;
    Ok(client)
}

fn print_info(message: &str) {
    info!("\n\n--- {message} ---\n");
}
