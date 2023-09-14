use server::components::config_provider::{ConfigProvider, FileConfigProvider};
use std::path::{Path, PathBuf};

fn file_exists(file_path: &str) -> bool {
    Path::new(file_path).is_file()
}

fn get_root_path() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set!");
    PathBuf::from(manifest_dir)
}

async fn scenario(extension: &str) {
    let mut config_path = get_root_path().join("../configs/server");
    assert!(config_path.set_extension(extension), "Cannot set extension");
    let config_path = config_path
        .to_str()
        .expect("Failed to convert config file to String")
        .to_owned();
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
    scenario("toml").await;
}

#[tokio::test]
async fn validate_server_config_json_from_repository() {
    scenario("json").await;
}
