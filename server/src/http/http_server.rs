use crate::http::{
    consumer_groups, consumer_offsets, messages, partitions, streams, system, topics, users,
};
use axum::http::Method;
use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use std::path::PathBuf;
use std::sync::Arc;
use streaming::systems::system::System;
use tokio::sync::RwLock;
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::server_config::{CorsConfig, HttpConfig};
use tracing::info;

pub async fn start(config: HttpConfig, system: Arc<RwLock<System>>) {
    let api_name = match config.tls.enabled {
        true => "HTTP API (TLS)",
        false => "HTTP API",
    };

    let mut app = Router::new().nest(
        "/",
        system::router(system.clone())
            .nest("/users", users::router(system.clone()))
            .nest(
                "/streams",
                streams::router(system.clone()).nest(
                    "/:stream_id/topics",
                    topics::router(system.clone())
                        .nest(
                            "/:topic_id/consumer-groups",
                            consumer_groups::router(system.clone()),
                        )
                        .nest("/:topic_id/messages", messages::router(system.clone()))
                        .nest(
                            "/:topic_id/consumer-offsets",
                            consumer_offsets::router(system.clone()),
                        )
                        .nest("/:topic_id/partitions", partitions::router(system.clone())),
                ),
            ),
    );

    if config.cors.enabled {
        app = app.layer(configure_cors(config.cors));
    }

    info!("Started {api_name} on: {:?}", config.address);

    if !config.tls.enabled {
        axum::Server::bind(&config.address.parse().unwrap())
            .serve(app.into_make_service())
            .await
            .unwrap();
        return;
    }

    let tls_config = RustlsConfig::from_pem_file(
        PathBuf::from(config.tls.cert_file),
        PathBuf::from(config.tls.key_file),
    )
    .await
    .unwrap();

    axum_server::bind_rustls(config.address.parse().unwrap(), tls_config)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

fn configure_cors(config: CorsConfig) -> CorsLayer {
    let allowed_origins = match config.allowed_origins {
        origins if origins.is_empty() => AllowOrigin::default(),
        origins if origins.first().unwrap() == "*" => AllowOrigin::any(),
        origins => AllowOrigin::list(origins.iter().map(|s| s.parse().unwrap())),
    };

    let allowed_headers = config
        .allowed_headers
        .iter()
        .map(|s| s.parse().unwrap())
        .collect::<Vec<_>>();

    let exposed_headers = config
        .exposed_headers
        .iter()
        .map(|s| s.parse().unwrap())
        .collect::<Vec<_>>();

    let allowed_methods = config
        .allowed_methods
        .iter()
        .map(|s| match s.to_uppercase().as_str() {
            "GET" => Method::GET,
            "POST" => Method::POST,
            "PUT" => Method::PUT,
            "DELETE" => Method::DELETE,
            "HEAD" => Method::HEAD,
            "OPTIONS" => Method::OPTIONS,
            "CONNECT" => Method::CONNECT,
            "PATCH" => Method::PATCH,
            "TRACE" => Method::TRACE,
            _ => panic!("Invalid HTTP method: {}", s),
        })
        .collect::<Vec<_>>();

    CorsLayer::new()
        .allow_methods(allowed_methods)
        .allow_origin(allowed_origins)
        .allow_headers(allowed_headers)
        .expose_headers(exposed_headers)
        .allow_credentials(config.allow_credentials)
        .allow_private_network(config.allow_private_network)
}
