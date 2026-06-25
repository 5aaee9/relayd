# Architecture

- SQLite stores allocation rows plus optional binding rows.
- Runtime manager owns data-plane lifecycle and runtime snapshots.
- HTTP API mutates SQLite then applies runtime updates with bounded waiting.
- Allocation and binding are modeled separately: unbound allocations keep the relay port reserved, while binding changes drive upstream host/port activation.
- Startup performs SQLite self-check, migrations/bootstrap, runtime start, restore sweep, then HTTP listen.
- `RELAYD_RUNTIME_MODE=proxy` is the default. It starts relayd-owned TCP and UDP listeners; TCP accepts connections and forwards to upstream, and UDP maintains per-client upstream sessions with TTL cleanup.
- `RELAYD_RUNTIME_MODE=netlink` starts no per-allocation relay sockets. It projects bound allocations into nftables DNAT rules through libnftnl/libmnl when the binary is built with the `netlink` Cargo feature.
- Netlink mode owns one configured nftables `inet` table and `nat` prerouting chain. Startup creates the table/chain when missing, flushes the chain, and rewrites rules from SQLite allocations. Runtime create/update/delete/restore rewrites the full chain from in-memory allocation state.
- In netlink mode, unbound allocations install no DNAT rule; `both` allocations expand to separate TCP and UDP rules; IPv4 and IPv6 targets are both supported.
- Netlink listener metrics are empty because forwarding happens in the kernel rather than relayd-owned sockets.
