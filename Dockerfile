# ---
# --- Builder image
# ---

FROM alpine:3 AS builder
RUN apk add curl gcc musl-dev pkgconfig

# Install cargo nightly, which is required for the "sparse-registry" feature
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly --profile minimal
ENV PATH=/root/.cargo/bin:$PATH

# Setup build env
ENV RUSTFLAGS="--cfg unsound_local_offset"
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

FROM alpine:3

# Set local timezone
RUN apk add tzdata                                     \
 && cp /usr/share/zoneinfo/Europe/Paris /etc/localtime \
 && echo "Europe/Paris" >  /etc/timezone               \
 && apk del tzdata

COPY --from=builder /srv/server /srv/server
ENTRYPOINT ["/srv/server"]
