use crate::configs::http::{HttpConfig, HttpCorsConfig};
use crate::http::jwt::cleaner::start_expired_tokens_cleaner;
use crate::http::jwt::jwt_manager::JwtManager;
use crate::http::jwt::middleware::jwt_auth;
use crate::http::metrics::metrics;
use crate::http::state::AppState;
use crate::http::{
    consumer_groups, consumer_offsets, messages, partitions, personal_access_tokens, streams,
    system, topics, users,
};
use crate::streaming::systems::system::SharedSystem;
use axum::http::Method;
use axum::{middleware, Router};
use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::info;

pub async fn start(config: HttpConfig, system: SharedSystem) {
    let api_name = if config.tls.enabled {
        "HTTP API (TLS)"
    } else {
        "HTTP API"
    };

    let app_state = build_app_state(&config, system).await;
    let mut app = Router::new().nest(
        "/",
        system::router(app_state.clone(), &config.metrics)
            .nest(
                "/personal-access-tokens",
                personal_access_tokens::router(app_state.clone()),
            )
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

    if config.metrics.enabled {
        app = app.layer(middleware::from_fn_with_state(app_state.clone(), metrics));
    }

    if config.cors.enabled {
        app = app.layer(configure_cors(config.cors));
    }

    start_expired_tokens_cleaner(app_state.clone());
    app = app.layer(middleware::from_fn_with_state(app_state.clone(), jwt_auth));
    info!("Started {api_name} on: {:?}", config.address);

    if !config.tls.enabled {
        axum::Server::bind(&config.address.parse().unwrap())
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
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
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .unwrap();
}

async fn build_app_state(config: &HttpConfig, system: SharedSystem) -> Arc<AppState> {
    let db;
    {
        let system_read = system.read().await;
        db = system_read
            .db
            .as_ref()
            .expect("Database not initialized")
            .clone();
    }

    let jwt_manager = JwtManager::from_config(&config.jwt, db);
    if let Err(error) = jwt_manager {
        panic!("Failed to initialize JWT manager: {}", error);
    }

    let jwt_manager = jwt_manager.unwrap();
    if jwt_manager.load_revoked_tokens().await.is_err() {
        panic!("Failed to load revoked access tokens");
    }

    Arc::new(AppState {
        jwt_manager,
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
