use gbfs_watcher::gbfs::api::GbfsApi;

const BASE_URL: &str =
    "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/gbfs.json";

#[tokio::main(flavor = "current_thread")]
async fn main() {
    match GbfsApi::new(BASE_URL).await {
        Err(err) => println!("{err}"),
        Ok(x) => println!("{:?}", x.get_system_information().await.unwrap()),
    }
}
