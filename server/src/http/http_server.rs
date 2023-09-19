use crate::configs::http::{HttpConfig, HttpCorsConfig};
use crate::http::jwt::{jwt_auth, no_jwt_auth, JwtManager};
use crate::http::state::AppState;
use crate::http::{
    consumer_groups, consumer_offsets, messages, partitions, streams, system, topics, users,
};
use crate::streaming::systems::system::System;
use axum::http::Method;
use axum::{middleware, Router};
use axum_server::tls_rustls::RustlsConfig;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::info;

pub async fn start(config: HttpConfig, system: Arc<RwLock<System>>) {
    let api_name = if config.tls.enabled {
        "HTTP API (TLS)"
    } else {
        "HTTP API"
    };

    let app_state = build_app_state(&config, system);
    let mut app = Router::new().nest(
        "/",
        system::router(app_state.clone())
            .nest("/users", users::router(app_state.clone()))
            .nest(
                "/streams",
                streams::router(app_state.clone()).nest(
                    "/:stream_id/topics",
                    topics::router(app_state.clone())
                        .nest(
                            "/:topic_id/consumer-groups",
                            consumer_groups::router(app_state.clone()),
                        )
                        .nest("/:topic_id/messages", messages::router(app_state.clone()))
                        .nest(
                            "/:topic_id/consumer-offsets",
                            consumer_offsets::router(app_state.clone()),
                        )
                        .nest(
                            "/:topic_id/partitions",
                            partitions::router(app_state.clone()),
                        ),
                ),
            ),
    );

    if config.cors.enabled {
        app = app.layer(configure_cors(config.cors));
    }

    {
        let system = app_state.system.read().await;
        if system.config.user.authentication_enabled {
            app = app.layer(middleware::from_fn_with_state(app_state.clone(), jwt_auth));
        } else {
            app = app.layer(middleware::from_fn(no_jwt_auth));
        }
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

fn build_app_state(config: &HttpConfig, system: Arc<RwLock<System>>) -> Arc<AppState> {
    if config.jwt.secret.is_empty() {
        panic!("JWT secret is empty");
    }

    Arc::new(AppState {
        jwt_manager: JwtManager::new(&config.jwt.secret, config.jwt.expiry),
        system,
    })
}

fn configure_cors(config: HttpCorsConfig) -> CorsLayer {
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
