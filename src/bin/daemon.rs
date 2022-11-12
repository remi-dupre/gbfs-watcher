use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::stream::StreamExt;
use tokio::sync::RwLock;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

use gbfs_watcher::gbfs::api::GbfsApi;
use gbfs_watcher::gbfs::models;
use gbfs_watcher::storage::journal::StationStatusJournal;

const BASE_URL: &str =
    "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/gbfs.json";

async fn update(
    api: &GbfsApi,
    journals_dir: &Path,
    journals: &RwLock<HashMap<models::StationId, RwLock<StationStatusJournal>>>,
) -> usize {
    let error_count = AtomicUsize::new(0);
    let updated_count = AtomicUsize::new(0);

    let total_count = futures::stream::iter(api.get_station_status().await.unwrap())
        .map(|status| {
            let error_count = &error_count;
            let updated_count = &updated_count;
            let journals_dir = &journals_dir;
            let journals = &journals;

            async move {
                if !journals.read().await.contains_key(&status.station_id) {
                    let mut guard = journals.write().await;

                    if let Entry::Vacant(e) = guard.entry(status.station_id) {
                        let path = journals_dir.join(format!("{}.journal", status.station_id));

                        let journal = StationStatusJournal::open(path)
                            .await
                            .expect("could not open journal");

                        e.insert(RwLock::new(journal));
                    }
                }

                let journals = journals.read().await;
                let mut journal = journals[&status.station_id].write().await;

                match journal.insert(status).await {
                    Err(err) => {
                        error_count.fetch_add(1, Ordering::Relaxed);
                        error!("Could not insert in journal: {err}")
                    }
                    Ok(true) => {
                        updated_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(false) => {}
                }
            }
        })
        .buffered(64)
        .count()
        .await;

    let updated_count = updated_count.into_inner();
    let error_count = error_count.into_inner();
    info!("Updated {updated_count}/{total_count} stations ({error_count} errors)");
    updated_count
}

#[tokio::main(flavor = "current_thread")]
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

    let api = GbfsApi::new(BASE_URL)
        .await
        .expect("could not connect to API");

    let journals = RwLock::new(HashMap::new());
    let mut timer = tokio::time::interval(Duration::from_secs(60));

    loop {
        timer.tick().await;
        update(&api, &journals_dir, &journals).await;
    }
}
