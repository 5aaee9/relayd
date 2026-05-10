# HTTP API

Canonical API documentation lives in [`docs/API.md`](../API.md).

Protocol values are `tcp`, `udp`, and `both`. `protocol = "both"` reserves one numeric relay port for both TCP and UDP, uses one shared binding target, and appears as one row in `/v1/allocations` and `/v1/ports`.

## Prometheus metrics

`GET /metrics` is the authenticated Prometheus scrape endpoint. It uses the same `Authorization: Bearer <token>` header as the `/v1` API but is not under the `/v1` prefix.

It returns `text/plain; version=0.0.4; charset=utf-8` with:

- `relayd_connections_current{port="<port>",protocol="tcp|udp"}`
- `relayd_rx_bytes_per_second{port="<port>",protocol="tcp|udp"}`
- `relayd_tx_bytes_per_second{port="<port>",protocol="tcp|udp"}`

For `protocol = "both"` allocations, Prometheus output contains separate `tcp` and `udp` series for the same port. Rx/tx speeds are byte-total deltas between scrapes; the first scrape for a label reports `0` speed.
