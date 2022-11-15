# ---
# --- Builder image
# ---

FROM debian:bullseye-slim AS builder
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y build-essential curl libssl-dev pkg-config

# Install cargo nightly
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly
ENV PATH=/root/.cargo/bin:$PATH

WORKDIR /srv
COPY . ./

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
