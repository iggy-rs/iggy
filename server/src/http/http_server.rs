use crate::configs::http::{HttpConfig, HttpCorsConfig};
use crate::http::diagnostics::request_diagnostics;
use crate::http::jwt::cleaner::start_expired_tokens_cleaner;
use crate::http::jwt::jwt_manager::JwtManager;
use crate::http::jwt::middleware::jwt_auth;
use crate::http::metrics::metrics;
use crate::http::shared::AppState;
use crate::http::{
    consumer_groups, consumer_offsets, messages, partitions, personal_access_tokens, streams,
    system, topics, users,
};
use crate::streaming::systems::system::SharedSystem;
use axum::http::Method;
use axum::{middleware, Router, ServiceExt};
use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;
use std::net::TcpListener as StdTcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener as TokioTcpListener;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{error, info};

/// Starts the HTTP API server.
/// Returns the address the server is listening on.
pub async fn start(config: HttpConfig, system: SharedSystem) -> SocketAddr {
    let api_name = if config.tls.enabled {
        "HTTP API (TLS)"
    } else {
        "HTTP API"
    };

    let app_state = build_app_state(&config, system).await;

    let system_routes = system::router(app_state.clone(), &config.metrics);
    let pat_routes = personal_access_tokens::router(app_state.clone());
    let user_routes = users::router(app_state.clone());
    let stream_routes = streams::router(app_state.clone());
    let topic_routes = topics::router(app_state.clone());
    let consumer_group_routes = consumer_groups::router(app_state.clone());
    let message_routes = messages::router(app_state.clone());
    let consumer_offset_routes = consumer_offsets::router(app_state.clone());
    let partition_routes = partitions::router(app_state.clone());

    let app = Router::new()
        .merge(system_routes)
        .merge(pat_routes)
        .merge(user_routes)
        .merge(stream_routes)
        .merge(topic_routes)
        .merge(consumer_group_routes)
        .merge(message_routes)
        .merge(consumer_offset_routes)
        .merge(partition_routes)
        .layer(middleware::from_fn_with_state(app_state.clone(), jwt_auth));

    let app = if config.cors.enabled {
        app.layer(configure_cors(config.cors))
    } else {
        app
    };

    let app = if config.metrics.enabled {
        app.layer(middleware::from_fn_with_state(app_state.clone(), metrics))
    } else {
        app
    };

    start_expired_tokens_cleaner(app_state.clone());
    let app = app.layer(middleware::from_fn(request_diagnostics));

    if !config.tls.enabled {
        let listener = TokioTcpListener::bind(config.address)
            .await
            .expect("Failed to bind TCP listener for HTTP API");
        let addr = listener
            .local_addr()
            .expect("Failed to get local address for HTTP API");
        info!("Started {api_name} on: {:?}", addr);
        tokio::task::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                error!("HTTP server error: {}", error);
            }
        });
        addr
    } else {
        let tls_config = RustlsConfig::from_pem_file(
            PathBuf::from(config.tls.cert_file),
            PathBuf::from(config.tls.key_file),
        )
        .await
        .unwrap();

        let listener = StdTcpListener::bind(config.address)
            .expect("Failed to bind TCP listener for HTTP TLS API");
        let addr = listener
            .local_addr()
            .expect("Failed to get local address for HTTP TLS API");

        info!("Started {api_name} on: {:?}", addr);

        tokio::task::spawn(async move {
            axum_server::from_tcp_rustls(listener, tls_config)
                .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .unwrap();
        });
        addr
    }
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
