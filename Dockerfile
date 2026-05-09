FROM alpine:3.20 AS builder

ARG ZIG_VERSION=0.16.0
ARG TARGETARCH

RUN apk add --no-cache \
    ca-certificates \
    curl \
    xz \
    sqlite-dev \
    libc6-compat \
    libstdc++ \
    build-base

RUN arch="${TARGETARCH}" \
    && if [ -z "$arch" ]; then arch="$(apk --print-arch)"; fi \
    && case "$arch" in \
        x86_64|amd64) zig_arch=x86_64 ;; \
        aarch64|arm64) zig_arch=aarch64 ;; \
        *) echo "unsupported TARGETARCH: $arch" >&2; exit 1 ;; \
    esac \
    && curl -L "https://ziglang.org/download/${ZIG_VERSION}/zig-${zig_arch}-linux-${ZIG_VERSION}.tar.xz" -o /tmp/zig.tar.xz \
    && tar -C /opt -xf /tmp/zig.tar.xz \
    && mv "/opt/zig-${zig_arch}-linux-${ZIG_VERSION}" /opt/zig \
    && ln -s /opt/zig/zig /usr/local/bin/zig

WORKDIR /src
COPY . .
RUN zig build -Doptimize=ReleaseSafe

FROM alpine:3.20 AS runtime

RUN apk add --no-cache \
    ca-certificates \
    sqlite-libs

WORKDIR /app
COPY --from=builder /src/zig-out/bin/relayd /usr/local/bin/relayd

ENV HTTP_LISTEN=:8080 \
    PORT_RANGE=10000-30000 \
    SQLITE_PATH=/data/relayd.sqlite3

EXPOSE 8080
VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/relayd"]
