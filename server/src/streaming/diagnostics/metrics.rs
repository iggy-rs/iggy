use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::registry::Registry;
use tracing::error;

// TODO: Extend this with more useful metrics
#[derive(Debug)]
pub(crate) struct Metrics {
    registry: Registry,
    http_requests: Counter,
}

impl Metrics {
    pub fn init() -> Self {
        let mut metrics = Metrics {
            registry: <Registry>::default(),
            http_requests: Counter::default(),
        };

        metrics.registry.register(
            "http_requests",
            "total count of HTTP requests",
            metrics.http_requests.clone(),
        );

        metrics
    }

    pub fn get_formatted_output(&self) -> String {
        let mut buffer = String::new();
        if let Err(err) = encode(&mut buffer, &self.registry) {
            error!("Failed to encode metrics: {}", err);
        }
        buffer
    }

    pub fn increment_http_requests(&self) {
        self.http_requests.inc();
    }
}
