use std::path::PathBuf;

use clap::Parser;
use futures::stream::StreamExt;
use serde::Serialize;
use signal_hook_tokio::Signals;
use tracing::{info, warn};

use gbfs_watcher::server::app::run_app;
use gbfs_watcher::server::state::State;

const VELIB_API_URL: &str =
    "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/gbfs.json";

#[derive(Debug, Parser, Serialize)]
#[command(author, version, about)]
pub struct Args {
    /// Directory where journal data is stored
    #[clap(short, long)]
    journals_dir: PathBuf,

    /// Directory where dumps are written
    #[clap(short, long)]
    dumps_dir: PathBuf,

    /// Port the API will listen to
    #[clap(short, long, default_value = "9000")]
    port: u16,

    /// Url to GBFS endpoint of the watched API
    #[clap(short, long, default_value = VELIB_API_URL)]
    watched_url: String,
}

#[tokio::main]
async fn main() {
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive({
            if cfg!(debug_assertions) {
                tracing_subscriber::filter::LevelFilter::DEBUG.into()
            } else {
                tracing_subscriber::filter::LevelFilter::INFO.into()
            }
        })
        .from_env()
        .expect("could not build filter");

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_target(false)
        .with_env_filter(filter)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting global subscriber failed");

    let args = Args::parse();
    let display_args = serde_json::to_string_pretty(&args).expect("could not display args");
    info!("Running with parameters: {display_args}");

    let mut signals = Signals::new([signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM])
        .expect("could not subscribe to signals");

    tokio::spawn(async move {
        let state = State::new(&args.watched_url, args.journals_dir, args.dumps_dir)
            .await
            .expect("failed to init state");

        tokio::spawn(run_app(state, args.port));
    });

    if let Some(signal) = signals.next().await {
        info!("Received signal {signal}: exiting application");
    } else {
        warn!("Normal exit without receiving a signal");
    }
}
