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
