use crate::configs::server::ServerConfig;
use crate::server_error::ServerError;
use async_trait::async_trait;
use figment::{
    providers::{Format, Json, Toml},
    value::{Dict, Map as FigmentMap, Tag, Value as FigmentValue},
    Error, Figment, Metadata, Profile, Provider,
};
use std::{env, path::Path};
use toml::{map::Map, Value as TomlValue};
use tracing::info;

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

    fn insert_overridden_values_from_env(
        source: &Dict,
        target: &mut Dict,
        keys: Vec<String>,
        value: FigmentValue,
    ) {
        if keys.is_empty() {
            return;
        }

        let mut current_source = source;
        let mut current_target = target;

        for i in 0..keys.len() {
            let combined_keys = keys[i..].join("_");

            if current_source.contains_key(&combined_keys) {
                current_target.insert(combined_keys, value.clone());
                return;
            }

            let key = &keys[i];
            match current_source.get(key) {
                Some(FigmentValue::Dict(_, inner_source_dict)) => {
                    if !current_target.contains_key(key) {
                        current_target
                            .insert(key.clone(), FigmentValue::Dict(Tag::Default, Dict::new()));
                    }

                    if let Some(FigmentValue::Dict(_, ref mut actual_inner_target_dict)) =
                        current_target.get_mut(key)
                    {
                        current_source = inner_source_dict;
                        current_target = actual_inner_target_dict;
                    } else {
                        return;
                    }
                }
                _ => return,
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

    fn try_parse_value(value: &str) -> FigmentValue {
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
        let default_config = toml::to_string(&ServerConfig::default())
            .expect("Cannot serialize default ServerConfig. Something's terribly wrong.");
        let toml_value: TomlValue = toml::from_str(&default_config).unwrap();
        let mut source_dict = Dict::new();
        if let TomlValue::Table(table) = toml_value {
            Self::walk_toml_table_to_dict("", table, &mut source_dict);
        }

        let mut new_dict = Dict::new();
        for (key, value) in env::vars() {
            let env_key = key.to_uppercase();
            if !env_key.starts_with(self.prefix.as_str()) {
                continue;
            }
            let keys: Vec<String> = env_key[self.prefix.len()..]
                .split('_')
                .map(|k| k.to_lowercase())
                .collect();
            let env_var_value = Self::try_parse_value(&value);
            info!(
                "{} value changed to: {:?} from environment variable",
                env_key, value
            );
            Self::insert_overridden_values_from_env(
                &source_dict,
                &mut new_dict,
                keys.clone(),
                env_var_value.clone(),
            );
        }
        let mut data = FigmentMap::new();
        data.insert(Profile::default(), new_dict);

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

/// This does exactly the same as Figment does internally.
fn file_exists<P: AsRef<Path>>(path: P) -> bool {
    let path = path.as_ref();

    if path.is_absolute() {
        return path.is_file();
    }

    let cwd = match std::env::current_dir() {
        Ok(dir) => dir,
        Err(_) => return false,
    };

    let mut current_dir = cwd.as_path();
    loop {
        let file_path = current_dir.join(path);
        if file_path.is_file() {
            return true;
        }

        current_dir = match current_dir.parent() {
            Some(parent) => parent,
            None => return false,
        };
    }
}

#[async_trait]
impl ConfigProvider for FileConfigProvider {
    async fn load_config(&self) -> Result<ServerConfig, ServerError> {
        info!("Loading config from path: '{}'...", self.path);

        if !file_exists(&self.path) {
            return Err(ServerError::CannotLoadConfiguration(format!(
                "Cannot find configuration file at path: '{}'.",
                self.path,
            )));
        }

        let config_builder = Figment::new();
        let extension = self.path.split('.').last().unwrap_or("");
        let config_builder = match extension {
            "json" => config_builder.merge(Json::file(&self.path)),
            "toml" => config_builder.merge(Toml::file(&self.path)),
            e => {
                return Err(ServerError::CannotLoadConfiguration(format!("Cannot load configuration: invalid file extension: {e}, only .json and .toml are supported.")));
            }
        };

        let custom_env_provider = CustomEnvProvider::new("IGGY_");
        let config_result: Result<ServerConfig, figment::Error> =
            config_builder.merge(custom_env_provider).extract();

        match config_result {
            Ok(config) => {
                info!("Config loaded from path: '{}'", self.path);
                info!("Using Config: {}", config);
                Ok(config)
            }
            Err(figment_error) => Err(ServerError::CannotLoadConfiguration(format!(
                "Failed to load configuration: {}",
                figment_error
            ))),
        }
    }
}
