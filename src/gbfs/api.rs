use chrono::offset::Utc;
use thiserror::Error as ThisError;

use super::models;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("could not request API: {0}")]
    Http(#[from] reqwest::Error),
    #[error("no feed available in API response")]
    EmptyFeeds,
    #[error("missing feed: {0}")]
    MissingFeed(&'static str),
}

#[derive(Debug)]
pub struct GbfsApi {
    client: reqwest::Client,
    system_information_url: String,
    station_information_url: String,
    station_status_url: String,
}

impl GbfsApi {
    /// Init API by listing available feeds. Langs are not supported so an arbitrary one will be
    /// selected.
    pub async fn new(gbfs_url: &str) -> Result<Self, Error> {
        let client = reqwest::Client::new();
        let resp: models::GbfsResponse = client.get(gbfs_url).send().await?.json().await?;

        let feeds = (resp.data.into_values())
            .next()
            .ok_or(Error::EmptyFeeds)?
            .feeds;

        let system_information_url = feeds
            .iter()
            .find(|feed| feed.name == "system_information")
            .ok_or(Error::MissingFeed("system_information"))?
            .url
            .clone();

        let station_information_url = feeds
            .iter()
            .find(|feed| feed.name == "station_information")
            .ok_or(Error::MissingFeed("station_information"))?
            .url
            .clone();

        let station_status_url = feeds
            .iter()
            .find(|feed| feed.name == "station_status")
            .ok_or(Error::MissingFeed("station_status"))?
            .url
            .clone();

        Ok(Self {
            client,
            system_information_url,
            station_information_url,
            station_status_url,
        })
    }

    pub async fn get_system_information(&self) -> Result<models::SystemInformation, Error> {
        let resp: models::SystemInformationResponse = self
            .client
            .get(&self.system_information_url)
            .send()
            .await?
            .json()
            .await?;

        Ok(resp.data)
    }

    pub async fn get_station_information(&self) -> Result<Vec<models::StationInformation>, Error> {
        let resp: models::StationInformationResponse = self
            .client
            .get(&self.station_information_url)
            .send()
            .await?
            .json()
            .await?;

        Ok(resp.data.stations)
    }

    pub async fn get_station_status(&self) -> Result<Vec<models::StationStatus>, Error> {
        let mut resp: models::StationStatusResponse = self
            .client
            .get(&self.station_status_url)
            .send()
            .await?
            .json()
            .await?;

        let now = Utc::now()
            .timestamp()
            .try_into()
            .expect("invalid timestamp");

        // Velib' API doesn't appear to return monotonic timestamps so we override them
        for status in &mut resp.data.stations {
            status.last_reported = now;
        }

        Ok(resp.data.stations)
    }
}
