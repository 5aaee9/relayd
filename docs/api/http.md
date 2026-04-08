# HTTP API

## Auth
Bearer token in `Authorization` header.

## Endpoints
- `POST /v1/ports`
- `POST /v1/ports/target`
- `POST /v1/ports/{id}`
- `DELETE /v1/ports/{id}`
- `GET /v1/ports`

## Response shape
List/update responses include:
- desired target: `host`, `target_port`
- effective target: `effective_host`, `effective_target_port`
- `runtime_status`
- `error_kind`
- `last_error`
