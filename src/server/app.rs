use std::net::SocketAddr;
use std::sync::Arc;

use axum::handler::Handler;
use axum::routing::get;
use axum::Router;
use tower_http::compression::{predicate, CompressionLayer, DefaultPredicate, Predicate};
use tower_http::trace::TraceLayer;

use crate::server::routes::handle_unmatched_path;

use super::routes;
use super::state::State;

pub async fn run_app(state: Arc<State>, port: u16) {
    let app = Router::new()
        .route(
            "/stations",
            get({
                let state = state.clone();
                move || routes::stations::get_stations(state)
            }),
        )
        .route(
            "/stations/:id",
            get({
                let state = state.clone();
                move |id| routes::stations::get_station_detail(state, id)
            }),
        )
        .route(
            "/stations/:id/history",
            get({
                let state = state.clone();
                move |id, params| routes::stations::get_station_history(state, id, params)
            }),
        )
        .route(
            "/dump/latest",
            get({
                let state = state.clone();
                move || routes::dump::latest_dump(state)
            }),
        )
        .fallback(handle_unmatched_path.into_service())
        .layer(
            CompressionLayer::new().compress_when(
                DefaultPredicate::new()
                    // Disable compression for already compressed responses
                    .and(predicate::NotForContentType::new("application/x-gzip")),
            ),
        )
        .layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .expect("HTTP server closed unexpectedly")
}
