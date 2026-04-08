# Architecture

- SQLite stores desired allocation state.
- Runtime manager owns listener lifecycle and runtime snapshots.
- HTTP API mutates SQLite then applies runtime updates with bounded waiting.
- Startup performs SQLite self-check, migrations/bootstrap, runtime start, restore sweep, then HTTP listen.
- TCP listener accepts connections and forwards to upstream; the active data path is the copy path, while splice remains a reserved Linux optimization direction.
- UDP listener maintains per-client upstream sessions with TTL cleanup.
