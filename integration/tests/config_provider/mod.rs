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
        config_provider.load_config().is_ok(),
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
    let expected_database_path = "awesome_database_path";
    let expected_http_enabled = false;
    let expected_tcp_enabled = "false";
    let expected_message_saver_enabled = false;
    let expected_message_expiry = "10s";

    env::set_var("IGGY_SYSTEM_DATABASE_PATH", expected_database_path);
    env::set_var("IGGY_HTTP_ENABLED", expected_http_enabled.to_string());
    env::set_var("IGGY_TCP_ENABLED", expected_tcp_enabled);
    env::set_var(
        "IGGY_MESSAGE_SAVER_ENABLED",
        expected_message_saver_enabled.to_string(),
    );
    env::set_var("IGGY_SYSTEM_RETENTION_POLICY_MESSAGE_EXPIRY", "10s");

    let config_path = get_root_path().join("../configs/server.toml");
    let file_config_provider = FileConfigProvider::new(config_path.as_path().display().to_string());
    let config = file_config_provider
        .load_config()
        .expect("Failed to load default server.toml config");

    assert_eq!(config.system.database.path, expected_database_path);
    assert_eq!(config.http.enabled, expected_http_enabled);
    assert_eq!(config.tcp.enabled.to_string(), expected_tcp_enabled);
    assert_eq!(config.message_saver.enabled, expected_message_saver_enabled);
    assert_eq!(
        config.system.retention_policy.message_expiry.to_string(),
        expected_message_expiry
    );

    env::remove_var("IGGY_SYSTEM_DATABASE_PATH");
    env::remove_var("IGGY_HTTP_ENABLED");
    env::remove_var("IGGY_TCP_ENABLED");
    env::remove_var("IGGY_MESSAGE_SAVER_ENABLED");
    env::remove_var("IGGY_SYSTEM_RETENTION_POLICY_MESSAGE_EXPIRY");
}
