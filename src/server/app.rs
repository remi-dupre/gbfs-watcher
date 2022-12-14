// gbfs-watcher: API and logger for GBFS endpoints
// Copyright (C) 2022  Rémi Dupré
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::handler::Handler;
use axum::routing::get;
use tower_http::compression::{predicate, CompressionLayer, DefaultPredicate, Predicate};
use tower_http::trace::TraceLayer;

use super::routes;
use super::routes::{handle_unmatched_path, DocumentationBuilder};
use super::state::State;

pub async fn run_app(state: Arc<State>, port: u16) {
    let (router, routes_doc) = DocumentationBuilder::default()
        .route(
            "/stations",
            "list stations on the network and get their current status",
            get({
                let state = state.clone();
                move || routes::stations::get_stations(state)
            }),
        )
        .route(
            "/stations/:id",
            "get current status of a station",
            get({
                let state = state.clone();
                move |id| routes::stations::get_station_detail(state, id)
            }),
        )
        .route(
            "/stations/:id/history",
            "get status history of a station",
            get({
                let state = state.clone();
                move |id, params| routes::stations::get_station_history(state, id, params)
            }),
        )
        .route(
            "/dumps",
            "list available dumps",
            get({
                let state = state.clone();
                move || routes::dump::list_dumps(state)
            }),
        )
        .route(
            "/dumps/:name",
            "download a dump by name",
            get({
                let state = state.clone();
                move |name| routes::dump::dump_by_name(state, name)
            }),
        )
        .route(
            "/dumps/latest",
            "download latest dump",
            get({
                let state = state.clone();
                move || routes::dump::latest_dump(state)
            }),
        )
        .into_parts();

    let app = router
        .fallback((move |request| handle_unmatched_path(routes_doc, request)).into_service())
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
