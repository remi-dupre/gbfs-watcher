use std::io::{Cursor, Write};

use crate::gbfs::models;

pub const STATION_STATUS_BIN_SIZE: usize = 27;

pub trait Binary<const BIN_SIZE: usize> {
    fn serialize(&self) -> [u8; BIN_SIZE];
    fn deserialize(buf: &[u8; BIN_SIZE]) -> Self;
}

impl Binary<STATION_STATUS_BIN_SIZE> for models::StationStatus {
    fn serialize(&self) -> [u8; STATION_STATUS_BIN_SIZE] {
        let &models::StationStatus {
            station_id,
            num_bikes_available,
            num_docks_available,
            num_docks_disabled,
            is_installed,
            is_returning,
            is_renting,
            last_reported,
            num_bikes_available_types,
        } = self;

        let bools_bitmask =
            (is_installed as u8) + ((is_returning as u8) << 1) + ((is_renting as u8) << 2);

        let mut buf = Cursor::new([0; STATION_STATUS_BIN_SIZE]);

        buf.write_all(&station_id.to_be_bytes())
            .expect("buffer is full");
        buf.write_all(&num_bikes_available.to_be_bytes())
            .expect("buffer is full");
        buf.write_all(&num_docks_available.to_be_bytes())
            .expect("buffer is full");
        buf.write_all(&num_docks_disabled.to_be_bytes())
            .expect("buffer is full");
        buf.write_all(&[bools_bitmask]).expect("buffer is full");
        buf.write_all(&last_reported.to_be_bytes())
            .expect("buffer is full");
        buf.write_all(&num_bikes_available_types.mechanical.to_be_bytes())
            .expect("buffer is full");
        buf.write_all(&num_bikes_available_types.ebike.to_be_bytes())
            .expect("buffer is full");

        assert_eq!(
            buf.position() as usize,
            STATION_STATUS_BIN_SIZE,
            "buffer is not full"
        );

        buf.into_inner()
    }

    fn deserialize(buf: &[u8; STATION_STATUS_BIN_SIZE]) -> Self {
        let (station_id, buf) = buf.split_at(8);
        let (num_bikes_available, buf) = buf.split_at(2);
        let (num_docks_available, buf) = buf.split_at(2);
        let (num_docks_disabled, buf) = buf.split_at(2);
        let (bools_bitmask, buf) = buf.split_at(1);
        let (last_reported, buf) = buf.split_at(8);
        let (num_bikes_available_mechanical, num_bikes_available_ebike) = buf.split_at(2);

        let station_id =
            models::StationId::from_be_bytes(station_id.try_into().expect("buffer is too small"));

        let num_bikes_available = models::VehicleCount::from_be_bytes(
            num_bikes_available.try_into().expect("buffer is too small"),
        );

        let num_docks_available = models::VehicleCount::from_be_bytes(
            num_docks_available.try_into().expect("buffer is too small"),
        );

        let num_docks_disabled = models::VehicleCount::from_be_bytes(
            num_docks_disabled.try_into().expect("buffer is too small"),
        );

        let [bools_bitmask] = bools_bitmask else { panic!("buffer is too small") };
        let is_installed = (bools_bitmask & 0b0000_0001) != 0;
        let is_returning = (bools_bitmask & 0b0000_0010) != 0;
        let is_renting = (bools_bitmask & 0b0000_0100) != 0;

        let last_reported = models::Timestamp::from_be_bytes(
            last_reported.try_into().expect("buffer is too small"),
        );

        let num_bikes_available_mechanical = models::VehicleCount::from_be_bytes(
            num_bikes_available_mechanical
                .try_into()
                .expect("buffer is too small"),
        );

        let num_bikes_available_ebike = models::VehicleCount::from_be_bytes(
            num_bikes_available_ebike
                .try_into()
                .expect("buffer is too small"),
        );

        models::StationStatus {
            station_id,
            num_bikes_available,
            num_docks_available,
            num_docks_disabled,
            is_installed,
            is_returning,
            is_renting,
            last_reported,
            num_bikes_available_types: models::BikesAvailablePerType {
                mechanical: num_bikes_available_mechanical,
                ebike: num_bikes_available_ebike,
            },
        }
    }
}
