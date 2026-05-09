# HTTP API

Canonical API documentation lives in [`docs/API.md`](../API.md).

Protocol values are `tcp`, `udp`, and `both`. `protocol = "both"` reserves one numeric relay port for both TCP and UDP, uses one shared binding target, and appears as one row in `/v1/allocations` and `/v1/ports`.
