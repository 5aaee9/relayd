FROM alpine:3.20 AS runtime

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY dist/relayd /usr/local/bin/relayd

ENV HTTP_LISTEN=0.0.0.0:8080 \
    PORT_RANGE=10000-30000 \
    SQLITE_PATH=/data/relayd.sqlite3

EXPOSE 8080
VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/relayd"]
