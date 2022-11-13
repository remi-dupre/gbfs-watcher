use std::net::SocketAddr;
use std::sync::Arc;

use axum::routing::get;
use axum::{Extension, Router};
use tower_http::compression::CompressionLayer;
use tower_http::trace::TraceLayer;

use super::routes;
use super::state::State;

pub async fn run_app(state: Arc<State>, port: u16) {
    let app = Router::new()
        .route("/stations", get(routes::stations::get_stations))
        .route("/stations/:id", get(routes::stations::get_station_detail))
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(Extension(state));

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    tracing::info!("listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .expect("HTTP server closed unexpectedly");
}
