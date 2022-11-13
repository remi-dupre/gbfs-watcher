use std::path::PathBuf;

use futures::stream::StreamExt;
use signal_hook_tokio::Signals;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use gbfs_watcher::server::app::run_app;
use gbfs_watcher::server::state::State;

const BASE_URL: &str =
    "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/gbfs.json";

#[tokio::main]
async fn main() {
    tracing::subscriber::set_global_default(
        FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .finish(),
    )
    .expect("setting default subscriber failed");

    tokio::spawn(async {
        let journals_dir: PathBuf = std::env::args()
            .nth(1)
            .expect("missing PATH parameter")
            .into();

        let state = State::new(BASE_URL, journals_dir)
            .await
            .expect("failed to init state");

        tokio::spawn(run_app(state, 3000));
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
