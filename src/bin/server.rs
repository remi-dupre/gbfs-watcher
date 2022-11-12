use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::http::header::HeaderName;
use axum::http::Request;
use axum::routing::get;
use axum::Router;
use futures::stream::StreamExt;
use signal_hook_tokio::Signals;
use tower_http::compression::CompressionLayer;
use tower_http::request_id::{
    MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer,
};
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use gbfs_watcher::server::state::State;

const BASE_URL: &str =
    "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/gbfs.json";

async fn root() -> String {
    let res = "hello, world!\n".repeat(10000);
    info!("Res is {} bytes", res.len());
    res
}

async fn error() -> Result<&'static str, &'static str> {
    Err("this is an error")
}

#[derive(Clone, Default)]
struct MyMakeRequestId {
    counter: Arc<AtomicU64>,
}

impl MakeRequestId for MyMakeRequestId {
    fn make_request_id<B>(&mut self, _request: &Request<B>) -> Option<RequestId> {
        let request_id = self
            .counter
            .fetch_add(1, Ordering::SeqCst)
            .to_string()
            .parse()
            .unwrap();

        Some(RequestId::new(request_id))
    }
}

#[tokio::main]
async fn main() {
    tracing::subscriber::set_global_default(
        FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .finish(),
    )
    .expect("setting default subscriber failed");

    let journals_dir: PathBuf = std::env::args()
        .nth(1)
        .expect("missing PATH parameter")
        .into();

    tokio::spawn(async move {
        let _state = State::new(BASE_URL, journals_dir)
            .await
            .expect("failed to init state");

        // Spawn web server
        let x_request_id = HeaderName::from_static("x-request-id");

        let app = Router::new()
            .route("/", get(root))
            .route("/error", get(error))
            .layer(SetRequestIdLayer::new(
                x_request_id.clone(),
                MyMakeRequestId::default(),
            ))
            .layer(PropagateRequestIdLayer::new(x_request_id))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(DefaultMakeSpan::new().include_headers(true))
                    .on_response(DefaultOnResponse::new().include_headers(true)),
            )
            .layer(CompressionLayer::new());

        let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
        tracing::info!("listening on {}", addr);

        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    let mut signals =
        Signals::new([signal_hook::consts::SIGINT]).expect("could not subscribe to signals");

    while let Some(signal) = signals.next().await {
        match signal {
            signal_hook::consts::SIGINT => {
                info!("SIGINT: exiting application");
                break;
            }
            _ => unreachable!("unexpected signal"),
        }
    }
}
