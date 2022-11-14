use std::path::PathBuf;

use clap::Parser;
use futures::stream::StreamExt;
use serde::Serialize;
use signal_hook_tokio::Signals;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

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

    /// Port the API will listen to
    #[clap(short, long, default_value = "9000")]
    port: u16,

    /// Url to GBFS endpoint of the watched API
    #[clap(short, long, default_value = VELIB_API_URL)]
    watched_url: String,
}

#[tokio::main]
async fn main() {
    tracing::subscriber::set_global_default(
        FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .finish(),
    )
    .expect("setting up default subscriber failed");

    let args = Args::parse();
    let display_args = serde_json::to_string_pretty(&args).expect("could not display args");
    info!("Running with parameters: {display_args}");

    let mut signals = Signals::new([signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM])
        .expect("could not subscribe to signals");

    tokio::spawn(async move {
        let state = State::new(&args.watched_url, args.journals_dir)
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
