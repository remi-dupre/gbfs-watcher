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

//! GBFS data schema, see https://gbfs.mobilitydata.org/specification/reference

use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::{AddAssign, DivAssign, Mul, MulAssign};

use serde::{Deserialize, Serialize};

pub type StationId = u64;
pub type Coord = f32;
pub type Timestamp = i64;
pub type TTL = u64;
pub type VehicleCount = u16;

/// Every JSON file presented in this specification contains the same common header information at
/// the top level of the JSON response object.
#[derive(Debug, Deserialize, Serialize)]
pub struct Response<D> {
    /// Indicates the last time data in the feed was updated. This timestamp represents the
    /// publisher's knowledge of the current state of the system at this point in time.
    #[serde(rename = "lastUpdatedOther")]
    pub last_updated: Timestamp,

    /// Number of seconds before the data in the feed will be updated again (0 if the data should
    /// always be refreshed).
    pub ttl: TTL,

    /// Response data in the form of name:value pairs.
    pub data: D,
}

/// GBFS
/// ----
///
/// Auto-discovery file that links to all of the other files published by the system.
pub type GbfsResponse = Response<FeedPerLang>;

/// The keys are the language that will be used throughout the rest of the files. It MUST match the
/// value in the system_information.json file.
pub type FeedPerLang = HashMap<String, FeedData>;

#[derive(Debug, Deserialize, Serialize)]
pub struct FeedData {
    /// An array of all of the feeds that are published by this auto-discovery file. Each element
    /// in the array is an object with the keys below.
    pub feeds: Vec<Feed>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Feed {
    /// Key identifying the type of feed this is. The key MUST be the base file name defined in the
    /// spec for the corresponding feed type (system_information for system_information.json file,
    /// station_information for station_information.json file).
    pub name: String,

    /// URL for the feed. Note that the actual feed endpoints (urls) may not be defined in the
    /// file_name.json format. For example, a valid feed endpoint could end with station_info
    /// instead of station_information.json.
    pub url: String,
}

/// System information
/// ------------------
pub type SystemInformationResponse = Response<SystemInformation>;

#[derive(Debug, Deserialize, Serialize)]
pub struct SystemInformation {
    /// This is a globally unique identifier for the vehicle share system. It is up to the
    /// publisher of the feed to guarantee uniqueness and MUST be checked against existing
    /// system_id fields in systems.csv to ensure this. This value is intended to remain the same
    /// over the life of the system.
    //
    /// Each distinct system or geographic area in which vehicles are operated SHOULD have its own
    /// system_id. System IDs SHOULD be recognizable as belonging to a particular system - for
    /// example, bcycle_austin or biketown_pdx - as opposed to random strings.
    pub system_id: String,

    /// Name of the system to be displayed to customers.
    pub name: String,

    /// The time zone where the system is located.
    pub timezone: String,

    /// The language that will be used throughout the rest of the files. It MUST match the value in
    /// the gbfs.json file.
    pub language: String,
}

/// Station information
/// -------------------
///
/// All stations included in station_information.json are considered public (meaning they can be
/// shown on a map for public use). If there are private stations (such as Capital Bikeshare’s
/// White House station), these SHOULD NOT be included here. Any station that is represented in
/// station_information.json MUST have a corresponding entry in station_status.json.
pub type StationInformationResponse = Response<StationInformationData>;

#[derive(Debug, Deserialize, Serialize)]
pub struct StationInformationData {
    /// Array that contains one object per station as defined below.
    pub stations: Vec<StationInformation>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct StationInformation {
    /// Identifier of a station.
    pub station_id: StationId,

    /// The public name of the station for display in maps, digital signage, and other text
    /// applications. Names SHOULD reflect the station location through the use of a cross street
    /// or local landmark. Abbreviations SHOULD NOT be used for names and other text (for example,
    /// "St." for "Street") unless a location is called by its abbreviated name (for example, “JFK
    /// Airport”).
    ///
    /// Examples:
    ///   - Broadway and East 22nd Street
    ///   - Convention Center
    ///   - Central Park South
    pub name: String,

    /// Latitude of the station in decimal degrees. This field SHOULD have a precision of 6 decimal
    /// places (0.000001).
    pub lat: Coord,

    /// Longitude of the station in decimal degrees. This field SHOULD have a precision of 6
    /// decimal places (0.000001).
    pub lon: Coord,

    /// Number of total docking points installed at this station, both available and unavailable,
    /// regardless of what vehicle types are allowed at each dock.
    ///
    /// If this is a virtual station defined using the is_virtual_station field, this number
    /// represents the total number of vehicles of all types that can be parked at the virtual
    /// station.
    ///
    /// If the virtual station is defined by station_area, this is the number that can park within
    /// the station area. If lat/lon are defined, this is the number that can park at those
    /// coordinates.
    pub capacity: VehicleCount,
}

/// Station status
/// --------------
///
/// Describes the capacity and rental availability of a station. Data returned SHOULD be as close
/// to realtime as possible, but in no case should it be more than 5 minutes out-of-date. See Data
/// Latency. Data reflects the operator's most recent knowledge of the station’s status. Any
/// station that is represented in station_status.json MUST have a corresponding entry in
/// station_information.json.
pub type StationStatusResponse = Response<StationStatusData>;

#[derive(Debug, Deserialize, Serialize)]
pub struct StationStatusData {
    /// Array that contains one object per station as defined below.
    pub stations: Vec<StationStatus>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct StationStatus<C = VehicleCount> {
    /// Identifier of a station.
    pub station_id: StationId,

    /// Number of bikes available for rental.
    pub num_bikes_available: C,

    /// Number of docks accepting bike returns.
    pub num_docks_available: C,

    /// Number of empty but disabled dock points at the station. This value remains as part of the
    /// spec as it is possibly useful during development.
    #[serde(default)]
    pub num_docks_disabled: C,

    /// 1/0 boolean - is the station currently on the street.
    #[serde(deserialize_with = "deserialize_bool_int")]
    pub is_installed: bool,

    /// 1/0 boolean - is the station currently renting bikes (even if the station is empty, if it
    /// is set to allow rentals this value should be 1).
    #[serde(deserialize_with = "deserialize_bool_int")]
    pub is_returning: bool,

    /// 1/0 boolean - is the station accepting bike returns (if a station is full but would allow a
    /// return if it was not full then this value should be 1).
    #[serde(deserialize_with = "deserialize_bool_int")]
    pub is_renting: bool,

    /// Integer POSIX timestamp indicating the last time this station reported its status to the
    /// backend.
    pub last_reported: Timestamp,

    /// This field is not part of the standart v1.1 schema
    #[serde(deserialize_with = "deserialize_bikes_available_per_type")]
    pub num_bikes_available_types: BikesAvailablePerType<C>,
}

impl<C: AddAssign<C>> AddAssign<StationStatus<C>> for StationStatus<C> {
    fn add_assign(&mut self, rhs: Self) {
        let Self {
            station_id: _,
            num_bikes_available,
            num_docks_available,
            num_docks_disabled,
            is_installed: _,
            is_returning: _,
            is_renting: _,
            last_reported: _,
            num_bikes_available_types,
        } = rhs;

        self.num_bikes_available += num_bikes_available;
        self.num_docks_available += num_docks_available;
        self.num_docks_disabled += num_docks_disabled;
        self.num_bikes_available_types.mechanical += num_bikes_available_types.mechanical;
        self.num_bikes_available_types.ebike += num_bikes_available_types.ebike;
    }
}

impl<C: Copy + MulAssign<C>> Mul<C> for StationStatus<C> {
    type Output = StationStatus<C>;

    fn mul(mut self, rhs: C) -> Self::Output {
        self *= rhs;
        self
    }
}

impl<C: Copy + MulAssign<C>> MulAssign<C> for StationStatus<C> {
    fn mul_assign(&mut self, rhs: C) {
        self.num_bikes_available *= rhs;
        self.num_docks_available *= rhs;
        self.num_docks_disabled *= rhs;
        self.num_bikes_available_types.mechanical *= rhs;
        self.num_bikes_available_types.ebike *= rhs;
    }
}

impl<C: Copy + DivAssign<C>> DivAssign<C> for StationStatus<C> {
    fn div_assign(&mut self, rhs: C) {
        self.num_bikes_available /= rhs;
        self.num_docks_available /= rhs;
        self.num_docks_disabled /= rhs;
        self.num_bikes_available_types.mechanical /= rhs;
        self.num_bikes_available_types.ebike /= rhs;
    }
}

impl From<StationStatus<VehicleCount>> for StationStatus<f32> {
    fn from(rhs: StationStatus<VehicleCount>) -> Self {
        let StationStatus {
            station_id,
            num_bikes_available,
            num_docks_available,
            num_docks_disabled,
            is_installed,
            is_returning,
            is_renting,
            last_reported,
            num_bikes_available_types,
        } = rhs;

        Self {
            station_id,
            num_bikes_available: num_bikes_available.into(),
            num_docks_available: num_docks_available.into(),
            num_docks_disabled: num_docks_disabled.into(),
            is_installed,
            is_returning,
            is_renting,
            last_reported,
            num_bikes_available_types: BikesAvailablePerType {
                mechanical: num_bikes_available_types.mechanical.into(),
                ebike: num_bikes_available_types.ebike.into(),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct BikesAvailablePerType<C = VehicleCount> {
    pub mechanical: C,
    pub ebike: C,
}

fn deserialize_bikes_available_per_type<'de, C, D>(
    deserializer: D,
) -> Result<BikesAvailablePerType<C>, D::Error>
where
    C: Default + Deserialize<'de>,
    D: serde::de::Deserializer<'de>,
{
    struct PerTypeVisitor<C>(PhantomData<C>);

    impl<'de, C: Default + Deserialize<'de>> serde::de::Visitor<'de> for PerTypeVisitor<C> {
        type Value = BikesAvailablePerType<C>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a sequence of maps")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut mechanical = C::default();
            let mut ebike = C::default();

            #[derive(Deserialize)]
            #[serde(rename_all = "lowercase")]
            enum TypedCount<X> {
                Mechanical(X),
                Ebike(X),
            }

            while let Some(x) = seq.next_element::<TypedCount<C>>()? {
                match x {
                    TypedCount::Mechanical(x) => mechanical = x,
                    TypedCount::Ebike(x) => ebike = x,
                };
            }

            Ok(BikesAvailablePerType { mechanical, ebike })
        }
    }

    let visitor = PerTypeVisitor(PhantomData::default());
    deserializer.deserialize_seq(visitor)
}

fn deserialize_bool_int<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let as_int: u8 = Deserialize::deserialize(deserializer)?;
    Ok(as_int != 0)
}
