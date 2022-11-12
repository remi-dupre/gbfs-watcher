use std::path::PathBuf;

use futures::stream::TryStreamExt;
use tokio::fs::File;

use gbfs_watcher::gbfs::models;
use gbfs_watcher::storage::journal::stream_from_current_pos;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let path: PathBuf = std::env::args()
        .nth(1)
        .expect("missing PATH parameter")
        .into();

    let journal = File::open(path).await.expect("could not open journal");

    stream_from_current_pos(journal)
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
