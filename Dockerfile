FROM rust:1.95-alpine AS builder

RUN apk add --no-cache \
    ca-certificates \
    musl-dev \
    pkgconfig \
    sqlite-dev

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bin relayd

FROM alpine:3.20 AS runtime

RUN apk add --no-cache \
    ca-certificates \
    sqlite-libs

WORKDIR /app
COPY --from=builder /src/target/release/relayd /usr/local/bin/relayd

ENV HTTP_LISTEN=0.0.0.0:8080 \
    PORT_RANGE=10000-30000 \
    SQLITE_PATH=/data/relayd.sqlite3

EXPOSE 8080
VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/relayd"]
