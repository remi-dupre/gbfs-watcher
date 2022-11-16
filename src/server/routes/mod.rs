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

pub mod dump;
pub mod stations;

use std::convert::Infallible;
use std::sync::Arc;

use axum::body::{Body, HttpBody};
use axum::http::Request;
use axum::response::Response;
use axum::Router;
use serde::Serialize;
use tower::Service;

use super::error::Error;

#[derive(Debug, Serialize)]
pub struct RouteDoc {
    pub url: &'static str,
    pub description: &'static str,
}

pub struct DocumentationBuilder<B> {
    endpoints: Vec<RouteDoc>,
    router: Router<B>,
}

impl<B: HttpBody + Send + 'static> DocumentationBuilder<B> {
    pub fn route<T>(self, url: &'static str, description: &'static str, handler: T) -> Self
    where
        T: Service<Request<B>, Response = Response, Error = Infallible> + Clone + Send + 'static,
        T::Future: Send + 'static,
    {
        let Self {
            mut endpoints,
            router,
        } = self;

        endpoints.push(RouteDoc { url, description });
        let router = router.route(url, handler);
        Self { endpoints, router }
    }

    pub fn into_parts(self) -> (Router<B>, Arc<Vec<RouteDoc>>) {
        (self.router, Arc::new(self.endpoints))
    }
}

impl<B: HttpBody + Send + 'static> Default for DocumentationBuilder<B> {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            router: Router::new(),
        }
    }
}

pub async fn handle_unmatched_path(routes: Arc<Vec<RouteDoc>>, request: Request<Body>) -> Error {
    let uri = request.uri().clone();
    let method = request.method().clone();

    Error::UnmatchedPath {
        uri,
        method,
        routes,
    }
}
