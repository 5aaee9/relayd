FROM rust:1.95-slim-bookworm AS builder

ARG ZIG_VERSION=0.16.0
ARG CARGO_ZIGBUILD_VERSION=0.22.3
ARG BUILD_TARGET=x86_64-unknown-linux-musl

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl xz-utils pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add "${BUILD_TARGET}" \
    && cargo install cargo-zigbuild --locked --version "${CARGO_ZIGBUILD_VERSION}"

RUN curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-x86_64-linux-${ZIG_VERSION}.tar.xz" -o /tmp/zig.tar.xz \
    && mkdir -p /opt/zig \
    && tar -xJf /tmp/zig.tar.xz -C /opt/zig --strip-components=1 \
    && ln -s /opt/zig/zig /usr/local/bin/zig \
    && rm /tmp/zig.tar.xz

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo zigbuild --locked --release --bin relayd --target "${BUILD_TARGET}"

FROM alpine:3.20 AS runtime

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /src/target/x86_64-unknown-linux-musl/release/relayd /usr/local/bin/relayd

ENV HTTP_LISTEN=0.0.0.0:8080 \
    PORT_RANGE=10000-30000 \
    SQLITE_PATH=/data/relayd.sqlite3

EXPOSE 8080
VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/relayd"]
