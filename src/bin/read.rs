use std::path::PathBuf;

use futures::stream::TryStreamExt;

use gbfs_watcher::gbfs::models;
use gbfs_watcher::storage::journal::Journal;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let path: PathBuf = std::env::args()
        .nth(1)
        .expect("missing PATH parameter")
        .into();

    let journal = Journal::open(&path).await.expect("could not open journal");

    journal
        .iter()
        .await
        .unwrap()
        .try_for_each(|obj: models::StationStatus| {
            let json = serde_json::to_string(&obj).expect("failed to build json");
            println!("{}", json);
            futures::future::ready(Ok(()))
        })
        .await
        .unwrap();
}
