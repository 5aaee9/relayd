# relayd

Linux-first Zig port-forwarder with:
- authenticated HTTP API
- SQLite persistence
- startup restore
- TCP forwarding with copy path + Linux splice fast-path structure
- UDP forwarding with async listener/session handling

## Env
- `HTTP_LISTEN` — HTTP API listen address, e.g. `:8080` or `127.0.0.1:8080`
- `PORT_RANGE` — default `10000-30000`
- `AUTH_TOKEN` — required bearer token
- `FORCE_TCP_COPY_FALLBACK` — optional `true/false`
- `RUNTIME_APPLY_TIMEOUT_MS` — optional, default `2000`
- `RESTORE_SWEEP_TIMEOUT_MS` — optional, default `30000`
- `SQLITE_PATH` — optional, default `relayd.sqlite3`

If `HTTP_LISTEN` is `:PORT`, relayd binds `127.0.0.1:PORT`.

## Build
```bash
zig build
zig build test
```

## Run
```bash
HTTP_LISTEN=:8080 AUTH_TOKEN=mytoken zig-out/bin/relayd
```

## API
### Create
`POST /v1/ports`
```json
{"protocol":"tcp","target_port":80}
```

### Set target host
`POST /v1/ports/target`
```json
{"id":"<uuid-v7>","host":"127.0.0.1"}
```

### Update
`POST /v1/ports/{id}`
```json
{"target_port":8080,"host":"127.0.0.1"}
```

### Delete
`DELETE /v1/ports/{id}`

### List
`GET /v1/ports`

All requests must send:
```text
Authorization: Bearer <AUTH_TOKEN>
```

## Notes
- `host` currently accepts IP literals only.
- Control-plane TLS is intentionally out of scope; run behind a trusted network or TLS reverse proxy.
