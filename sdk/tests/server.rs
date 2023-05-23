use sdk::client::Client;
use sdk::config::Config;
use shared::streams::create_stream::CreateStream;
use shared::streams::get_streams::GetStreams;
use tokio::fs;
use tokio::process::Command;
use tokio::runtime::Runtime;

#[tokio::test]
async fn stream_should_be_created() {
    let runtime = Runtime::new().unwrap();
    runtime.spawn(async {
        Command::new("cargo")
            .kill_on_drop(true)
            .args(&["r", "--manifest-path", "../server/Cargo.toml"])
            .spawn()
            .expect("Could not start server")
            .wait()
            .await
            .unwrap()
    });

    let files_path = "local_data";
    let client = Client::create(Config::default()).unwrap();
    let client = client.connect().await.unwrap();
    let streams = client.get_streams(&GetStreams {}).await.unwrap();
    assert!(streams.is_empty());

    let create_stream = CreateStream {
        stream_id: 1,
        name: "test".to_string(),
    };
    client.create_stream(&create_stream).await.unwrap();

    let streams = client.get_streams(&GetStreams {}).await.unwrap();
    assert_eq!(streams.len(), 1);

    let stream = streams.get(0).unwrap();
    assert_eq!(stream.id, create_stream.stream_id);
    assert_eq!(stream.name, create_stream.name);

    runtime.shutdown_background();

    fs::remove_dir_all(files_path).await.unwrap();
}
