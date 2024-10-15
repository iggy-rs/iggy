use crate::configs::server::TelemetryConfig;
use crate::configs::system::LoggingConfig;
use crate::server_error::ServerError;
use tracing_subscriber::prelude::*;

pub struct Logging {}

impl Logging {
    pub fn new(_: TelemetryConfig) -> Self {
        Self {}
    }

    pub fn early_init(&mut self) {
        let console_layer = console_subscriber::spawn();

        let subscriber = tracing_subscriber::registry().with(console_layer);

        tracing::subscriber::set_global_default(subscriber)
            .expect("Setting global default subscriber failed");
    }

    pub fn late_init(
        &mut self,
        _base_directory: String,
        _config: &LoggingConfig,
    ) -> Result<(), ServerError> {
        Ok(())
    }
}

impl Default for Logging {
    fn default() -> Self {
        Self::new(TelemetryConfig::default())
    }
}
