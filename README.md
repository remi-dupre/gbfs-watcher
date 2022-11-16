# GBFS Watcher

A daemon which logs [Velib'][velib] stations statuses and provides a REST
endpoint to access data. Velib' API is built over [GBFS v1][gbfs], so this API
could trivially be generalized to other mobility networks.

The goal of the project is to be as lightweight and portable as possible to
allow running it for years with decent latency on a Raspberry Pi, possibly
using a bad to average USB hard drive.

This requirement comes from a previous attempt for a similar project backed on
SQLite which became much too slow on such hardware after a few month of data
was ingested (SQLite despite its bad reputation should be quite competitive for
this task).

## Usage

I'm hosting an instance at https://velib-history-api.dupre.io, open the link to
see the list of available endpoints.

You can also host your own instance using methods below, use the _--help_
parameter to see extra options.

#### Using cargo

```
cargo run --release --bin server -- \
    --journals-dir <JOURNALS_DIR>   \
    --dumps-dir <DUMPS_DIR>
```

#### Using docker

```
docker run ghcr.io/remi-dupre/gbfs-watcher:latest \
    --journals-dir <JOURNALS_DIR>                 \
    --dumps-dir <DUMPS_DIR>
```

[gbfs]: https://github.com/MobilityData/gbfs/blob/v1.1/gbfs.md
[velib]: https://www.velib-metropole.fr
