use async_trait::async_trait;
use figment::{
    providers::{Format, Json, Toml},
    value::{Dict, Map as FigmentMap, Value as FigmentValue},
    Error, Figment, Metadata, Profile, Provider,
};
use std::env;
use toml::{map::Map, Value as TomlValue};
use tracing::info;

use crate::configs::server::ServerConfig;
use crate::server_error::ServerError;

const DEFAULT_CONFIG_PROVIDER: &str = "file";
const DEFAULT_CONFIG_PATH: &str = "configs/server.toml";

#[async_trait]
pub trait ConfigProvider {
    async fn load_config(&self) -> Result<ServerConfig, ServerError>;
}

#[derive(Debug)]
pub struct FileConfigProvider {
    path: String,
}

pub struct CustomEnvProvider {
    prefix: String,
}

impl FileConfigProvider {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}

impl CustomEnvProvider {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
        }
    }

    fn walk_toml_table_to_dict(prefix: &str, table: Map<String, TomlValue>, dict: &mut Dict) {
        for (key, value) in table {
            let new_prefix = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{}.{}", prefix, key)
            };
            match value {
                TomlValue::Table(inner_table) => {
                    let mut nested_dict = Dict::new();
                    Self::walk_toml_table_to_dict(&new_prefix, inner_table, &mut nested_dict);
                    dict.insert(key, FigmentValue::from(nested_dict));
                }
                _ => {
                    dict.insert(key, Self::toml_to_figment_value(&value));
                }
            }
        }
    }

    fn find_and_insert_nested_key(dict: &mut Dict, keys: Vec<String>, value: FigmentValue) {
        let mut current_dict = dict;
        for i in 0..keys.len() {
            let combined_keys = keys[i..].join("_");
            if let std::collections::btree_map::Entry::Occupied(mut e) =
                current_dict.entry(combined_keys)
            {
                e.insert(value);
                return;
            }

            let key = &keys[i];
            if let Some(FigmentValue::Dict(_, ref mut next_dict)) = current_dict.get_mut(key) {
                current_dict = next_dict;
            } else {
                return;
            }
        }
    }

    fn toml_to_figment_value(toml_value: &TomlValue) -> FigmentValue {
        match toml_value {
            TomlValue::String(s) => FigmentValue::from(s.clone()),
            TomlValue::Integer(i) => FigmentValue::from(*i),
            TomlValue::Float(f) => FigmentValue::from(*f),
            TomlValue::Boolean(b) => FigmentValue::from(*b),
            TomlValue::Array(arr) => {
                let vec: Vec<FigmentValue> = arr.iter().map(Self::toml_to_figment_value).collect();
                FigmentValue::from(vec)
            }
            TomlValue::Table(tbl) => {
                let mut dict = figment::value::Dict::new();
                for (key, value) in tbl.iter() {
                    dict.insert(key.clone(), Self::toml_to_figment_value(value));
                }
                FigmentValue::from(dict)
            }
            TomlValue::Datetime(_) => todo!("not implemented yet!"),
        }
    }

    fn try_parse_value(value: String) -> FigmentValue {
        if value == "true" {
            return FigmentValue::from(true);
        }
        if value == "false" {
            return FigmentValue::from(false);
        }
        if let Ok(int_val) = value.parse::<i64>() {
            return FigmentValue::from(int_val);
        }
        if let Ok(float_val) = value.parse::<f64>() {
            return FigmentValue::from(float_val);
        }
        FigmentValue::from(value)
    }
}
impl Provider for CustomEnvProvider {
    fn metadata(&self) -> Metadata {
        Metadata::named("iggy-server config")
    }

    fn data(&self) -> Result<FigmentMap<Profile, Dict>, Error> {
        let toml_value: TomlValue =
            toml::from_str(include_str!("../../../configs/server.toml")).unwrap();
        let mut toml_as_dict = Dict::new();
        if let TomlValue::Table(table) = toml_value {
            Self::walk_toml_table_to_dict("", table, &mut toml_as_dict);
        }
        for (key, value) in env::vars() {
            let env_key = key.to_uppercase();
            if !env_key.starts_with(self.prefix.as_str()) {
                continue;
            }
            let keys: Vec<String> = env_key[self.prefix.len()..]
                .split('_')
                .map(|k| k.to_lowercase())
                .collect();
            let env_var_value = Self::try_parse_value(value);
            info!(
                "{} value changed to: {:?} from environment variable",
                env_key, env_var_value
            );
            Self::find_and_insert_nested_key(&mut toml_as_dict, keys, env_var_value);
        }
        let mut data = FigmentMap::new();
        data.insert(Profile::default(), toml_as_dict);

        Ok(data)
    }
}

pub fn resolve(config_provider_type: &str) -> Result<Box<dyn ConfigProvider>, ServerError> {
    match config_provider_type {
        DEFAULT_CONFIG_PROVIDER => {
            let path =
                env::var("IGGY_CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());
            Ok(Box::new(FileConfigProvider::new(path)))
        }
        _ => Err(ServerError::InvalidConfigurationProvider(
            config_provider_type.to_string(),
        )),
    }
}

#[async_trait]
impl ConfigProvider for FileConfigProvider {
    async fn load_config(&self) -> Result<ServerConfig, ServerError> {
        info!("Loading config from path: '{}'...", self.path);
        let config_builder = Figment::new();
        let extension = self.path.split('.').last().unwrap_or("");
        let config_builder = match extension {
            "json" => config_builder.merge(Json::file(&self.path)),
            "toml" => config_builder.merge(Toml::file(&self.path)),
            _ => {
                return Err(ServerError::CannotLoadConfiguration("Cannot load configuration: invalid file extension, only .json and .toml are supported.".to_owned()));
            }
        };
        let custom_env_provider = CustomEnvProvider::new("IGGY_");
        let config_builder = config_builder.merge(custom_env_provider);
        match config_builder.extract::<ServerConfig>() {
            Ok(config) => {
                info!("Config loaded from path: '{}'", self.path);
                Ok(config)
            }
            Err(figment_error) => Err(ServerError::CannotLoadConfiguration(format!(
                "Failed to load configuration: {}",
                figment_error
            ))),
        }
    }
}
