use figment::providers::{Format, Json, Toml};
use figment::Figment;
use serde_json::Value;
use std::error;
use std::path::PathBuf;
use vergen_git2::{BuildBuilder, CargoBuilder, Emitter, Git2Builder, RustcBuilder, SysinfoBuilder};

const JSON_CONFIG_FILENAME: &str = "server.json";
const TOML_CONFIG_FILENAME: &str = "server.toml";

fn main() -> Result<(), Box<dyn error::Error>> {
    if option_env!("IGGY_CI_BUILD") == Some("true") {
        Emitter::default()
            .add_instructions(&BuildBuilder::all_build()?)?
            .add_instructions(&CargoBuilder::all_cargo()?)?
            .add_instructions(&Git2Builder::all_git()?)?
            .add_instructions(&RustcBuilder::all_rustc()?)?
            .add_instructions(&SysinfoBuilder::all_sysinfo()?)?
            .emit()?;

        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");

        println!(
            "cargo:rerun-if-changed={}",
            workspace_root
                .join("configs")
                .canonicalize()
                .unwrap()
                .display()
        );

        let json_config_path = workspace_root
            .clone()
            .join("configs")
            .join(JSON_CONFIG_FILENAME);
        let toml_config_path = workspace_root.join("configs").join(TOML_CONFIG_FILENAME);

        let json_config: Value = match Figment::new().merge(Json::file(json_config_path)).extract()
        {
            Ok(config) => config,
            Err(e) => panic!("Failed to load {JSON_CONFIG_FILENAME}: {e}"),
        };

        let toml_config: Value = match Figment::new().merge(Toml::file(toml_config_path)).extract()
        {
            Ok(config) => config,
            Err(e) => panic!("Failed to load {TOML_CONFIG_FILENAME}: {e}"),
        };

        compare_configs(&json_config, &toml_config, "");
    } else {
        println!("cargo:info=Skipping build script because CI environment variable IGGY_CI_BUILD is not set to 'true'");
    }

    Ok(())
}

fn compare_configs(json_value: &Value, toml_value: &Value, path: &str) {
    match (json_value, toml_value) {
        (Value::Object(json_map), Value::Object(toml_map)) => {
            for (key, j_val) in json_map {
                let new_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{} -> {}", path, key)
                };
                match toml_map.get(key) {
                    Some(t_val) => compare_configs(j_val, t_val, &new_path),
                    None => panic!(
                        "Key '{key}' found in {JSON_CONFIG_FILENAME} but not {TOML_CONFIG_FILENAME} \
                        at path: {path}"
                    ),
                }
            }
            for key in toml_map.keys() {
                if !json_map.contains_key(key) {
                    panic!(
                        "Key '{key}' found in {TOML_CONFIG_FILENAME} but not in {JSON_CONFIG_FILENAME} \
                        at path: {path}",
                    );
                }
            }
        }
        (Value::Array(j_arr), Value::Array(t_arr)) => {
            if j_arr.len() != t_arr.len() {
                panic!(
                    "Array length differs at path '{path}': {JSON_CONFIG_FILENAME} has {} elements, \
                    {TOML_CONFIG_FILENAME} has {} elements",
                    j_arr.len(),
                    t_arr.len()
                );
            } else {
                for (i, (j_val, t_val)) in j_arr.iter().zip(t_arr).enumerate() {
                    let new_path = format!("{}[{}]", path, i);
                    compare_configs(j_val, t_val, &new_path);
                }
            }
        }
        _ => {
            if json_value != toml_value {
                panic!(
                    "Difference in {JSON_CONFIG_FILENAME} vs {TOML_CONFIG_FILENAME} \
                    found at '{path}': JSON value: {:?}, TOML value: {:?}",
                    json_value, toml_value
                );
            }
        }
    }
}
