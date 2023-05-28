use axum::routing::get;
use axum::Router;

const NAME: &str = "Iggy HTTP";
const PONG: &str = "pong";

pub fn router() -> Router {
    let router = Router::new()
        .route("/", get(|| async { NAME }))
        .route("/ping", get(|| async { PONG }));
    #[cfg(feature = "allow_kill_command")]
    {
        router.route("/kill", axum::routing::post(kill))
    }

    #[cfg(not(feature = "allow_kill_command"))]
    router
}

#[cfg(feature = "allow_kill_command")]
async fn kill() -> axum::http::StatusCode {
    tokio::spawn(async move { std::process::exit(0) });
    axum::http::StatusCode::OK
}
