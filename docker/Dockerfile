FROM rust:1.75-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY docs ./docs
COPY docker/config ./docker/config

RUN cargo build --release

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y ca-certificates redis-tools && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/aster-proxy /usr/local/bin/aster-proxy
COPY docker/config/integration.toml /app/config.toml

EXPOSE 6380 6381 2110

CMD ["aster-proxy", "--config", "/app/config.toml"]
