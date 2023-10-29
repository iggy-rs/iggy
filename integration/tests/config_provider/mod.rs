use integration::file::{file_exists, get_root_path};
use serial_test::serial;
use server::configs::config_provider::{ConfigProvider, FileConfigProvider};
use std::env;

async fn scenario_parsing_from_file(extension: &str) {
    let mut config_path = get_root_path().join("../configs/server");
    assert!(config_path.set_extension(extension), "Cannot set extension");
    let config_path = config_path.as_path().display().to_string();
    let config_provider = FileConfigProvider::new(config_path.clone());
    assert!(
        file_exists(&config_path),
        "Config file not found: {}",
        config_path
    );
    assert!(
        config_provider.load_config().await.is_ok(),
        "ConfigProvider failed to parse config from {}",
        config_path
    );
}

#[tokio::test]
async fn validate_server_config_toml_from_repository() {
    scenario_parsing_from_file("toml").await;
}

#[tokio::test]
async fn validate_server_config_json_from_repository() {
    scenario_parsing_from_file("json").await;
}

// This test needs to be run in serial because it modifies the environment variables
// which are shared, since all tests run in parallel by default.
#[serial]
#[tokio::test]
async fn validate_custom_env_provider() {
    env::set_var("IGGY_SYSTEM_DATABASE_PATH", "awesome_database_path");
    env::set_var("IGGY_QUIC_DATAGRAM_SEND_BUFFER_SIZE", "1337");
    env::set_var("IGGY_QUIC_CERTIFICATE_SELF_SIGNED", "false");
    env::set_var("IGGY_HTTP_ENABLED", "false");
    env::set_var("IGGY_SYSTEM_PARTITION_MESSAGES_REQUIRED_TO_SAVE", "42");

    let config_path = get_root_path().join("../configs/server.toml");

    let file_config_provider = FileConfigProvider::new(config_path.as_path().display().to_string());
    let config = file_config_provider
        .load_config()
        .await
        .expect("Failed to load default server.toml config");

    assert_eq!(config.system.database.path, "awesome_database_path");
    assert_eq!(config.quic.datagram_send_buffer_size, 1337);
    assert!(!config.quic.certificate.self_signed);
    assert!(!config.http.enabled);
    assert_eq!(config.system.partition.messages_required_to_save, 42);

    env::remove_var("IGGY_SYSTEM_DATABASE_PATH");
    env::remove_var("IGGY_QUIC_DATAGRAM_SEND_BUFFER_SIZE");
    env::remove_var("IGGY_QUIC_CERTIFICATE_SELF_SIGNED");
    env::remove_var("IGGY_HTTP_ENABLED");
    env::remove_var("IGGY_SYSTEM_PARTITION_MESSAGES_REQUIRED_TO_SAVE");
}
