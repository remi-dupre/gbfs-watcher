# ---
# --- Builder image
# ---

FROM rust:slim-bullseye AS builder
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y libssl-dev pkg-config
WORKDIR /srv
COPY . ./

RUN rustup toolchain install nightly

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/srv/target               \
    cargo +nightly -Z sparse-registry build --profile production --bin server

RUN --mount=type=cache,target=/srv/target \
    cp target/production/server .

# ---
# --- Published image
# ---

FROM debian:bullseye-slim
RUN apt-get update && apt-get install openssl ca-certificates
ENV RUST_LOG "info"
COPY --from=builder /srv/server /srv/server
ENTRYPOINT ["/srv/server"]
