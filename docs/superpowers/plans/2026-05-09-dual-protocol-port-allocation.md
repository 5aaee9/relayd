# Dual-Protocol Port Allocation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `protocol = "both"` so one allocation ID reserves and manages TCP and UDP listeners on the same relay port.

**Architecture:** Extend the protocol model with `.both`; enforce cross-protocol port conflicts in service allocation selection; extend runtime listener registration so a single allocation entry dispatches each event by the concrete TCP or UDP FD; keep one binding row and one API row per dual allocation.

**Tech Stack:** Zig, in-repo SQLite wrapper, Linux epoll runtime, existing HTTP integration test harness, `zig build test`.

---

## File map

- Modify `src/model/allocation.zig`: add `.both` protocol parsing/serialization.
- Modify `src/service/allocation_service.zig`: perform protocol-aware conflict detection inside the allocation transaction.
- Modify `src/runtime/manager.zig`: allow one `ListenerEntry` to own TCP and UDP listener FDs; pass concrete listener FDs into TCP accept and UDP readable/GRO/io_uring paths.
- Modify `src/storage/sqlite.zig`: ensure ordering is stable and dual rows load correctly; add optional repository conflict helper only if service-level scan becomes too broad.
- Modify `src/http/server.zig`: handle `.both` in existing switches after model/runtime changes; no route shape changes expected.
- Modify `tests/unit/allocator_test.zig`: parser regression for `both`.
- Modify `tests/unit/sqlite_test.zig`: persistence/list ordering checks for `both`.
- Modify `tests/integration/http_api_test.zig`: HTTP create/list/read/compatibility checks for `both`.
- Modify `tests/integration/service_forward_test.zig`: runtime TCP+UDP forwarding, delete release, and restore checks for `both`.
- Modify `docs/API.md` and `docs/api/http.md`: public API documentation.
- Keep `docs/superpowers/specs/2026-05-09-dual-protocol-port-allocation-design.md` and this plan as workflow artifacts.

## Acceptance checklist from `dual-protocol-port-allocation.md`

- [ ] `POST /v1/allocations` with `{"protocol":"both"}` returns one allocation with one ID and one port.
- [ ] TCP and UDP both listen on the returned port.
- [ ] `GET /v1/allocations/{id}` returns `protocol = "both"`.
- [ ] `GET /v1/allocations` returns one row for the dual allocation.
- [ ] `GET /v1/ports` returns one compatibility row for the dual allocation.
- [ ] A `tcp` allocation cannot later reserve the same port as an existing `both` allocation.
- [ ] A `udp` allocation cannot later reserve the same port as an existing `both` allocation.
- [ ] A `both` allocation cannot reserve a port already used by either TCP or UDP.
- [ ] Deleting the dual allocation releases both TCP and UDP listeners, verified by reusing both protocols after deletion.
- [ ] Startup restore recreates both listeners for persisted dual allocations.
- [ ] Existing `tcp` and `udp` behavior remains unchanged.

## Task 1: Lock protocol and reservation semantics with failing tests

**Files:**
- Modify: `tests/unit/allocator_test.zig`
- Modify: `tests/unit/sqlite_test.zig`
- Modify: `tests/integration/http_api_test.zig`
- Modify: `tests/integration/service_forward_test.zig`

- [ ] **Step 1: Add protocol parser test**

Add to `test "protocol parser"` in `tests/unit/allocator_test.zig`:

```zig
try std.testing.expectEqual(model.Protocol.both, model.Protocol.fromString("both").?);
try std.testing.expectEqual(model.Protocol.both, model.Protocol.fromString("BOTH").?);
```

- [ ] **Step 2: Add HTTP allocation lifecycle assertions for both**

Add a new test in `tests/integration/http_api_test.zig` named `http dual protocol allocation endpoints return one aggregate row`. Use the existing `Harness` and `doHttp` helpers. The test must:

```zig
const create_resp = try doHttp(std.testing.allocator, http_port, "POST", "/v1/allocations", "{\"protocol\":\"both\"}");
try std.testing.expectEqual(@as(u16, 201), create_resp.status);
try std.testing.expect(std.mem.indexOf(u8, create_resp.body, "\"protocol\":\"both\"") != null);
const allocation_id = try extractJsonString(std.testing.allocator, create_resp.body, "id");
const allocation_port = try extractJsonU16(create_resp.body, "port");
```

Then GET `/v1/allocations/{id}`, GET `/v1/allocations`, and GET `/v1/ports`; assert each response is `200`, contains `"protocol":"both"`, contains the ID, and does not duplicate the ID in the response body. Also call compatibility `POST /v1/ports` with `{"protocol":"both","target_port":<port>}` and assert it returns `201`, seeds `target_port`, and appears as one `protocol":"both"` row in `GET /v1/ports`. Add a small helper if needed:

```zig
fn countOccurrences(haystack: []const u8, needle: []const u8) usize {
    var count: usize = 0;
    var rest = haystack;
    while (std.mem.indexOf(u8, rest, needle)) |idx| {
        count += 1;
        rest = rest[idx + needle.len ..];
    }
    return count;
}
```

- [ ] **Step 3: Add service conflict tests**

In `tests/integration/service_forward_test.zig`, add a test named `service dual protocol allocation conflicts with single protocol allocations`. Create services with a one-port range. Assert:

```zig
var both_alloc = try svc.createAllocation(.both, null);
defer both_alloc.deinit(std.testing.allocator);
try std.testing.expectError(error.NoAvailablePort, svc.createAllocation(.tcp, null));
try std.testing.expectError(error.NoAvailablePort, svc.createAllocation(.udp, null));
```

Then use fresh harnesses with one-port ranges for `tcp` first then `both`, and `udp` first then `both`, asserting `error.NoAvailablePort`.

- [ ] **Step 4: Add runtime forwarding test for both**

In `tests/integration/service_forward_test.zig`, add a test named `service forwards tcp and udp for dual protocol allocation on same port`. Reuse existing TCP echo and UDP echo helpers. Create a concrete helper `startDualProtocolEchoServer()` that binds a TCP listener and a UDP socket to the same target port on `127.0.0.1`. It can bind TCP to port 0, read its assigned port, then bind UDP to that same port; if UDP bind fails, close TCP and retry a bounded number of times. Create `var alloc = try svc.createAllocation(.both, null)`, call `try svc.putBinding(alloc.id, "127.0.0.1", dual_echo.port)`, then assert a TCP client and a UDP client both receive echo through the same `alloc.port`.

- [ ] **Step 5: Add delete-release and restore test coverage**

Add tests or extend the forwarding test to delete the dual allocation and then prove both protocol sockets are released: first create a TCP allocation on the same single-port range, delete it, then create a UDP allocation on that same range. Add a restore test that persists a `.both` allocation with a binding to `startDualProtocolEchoServer()`, calls `restoreAll()`, and verifies TCP and UDP forwarding on the restored relay port.

- [ ] **Step 6: Run failing focused tests**

Run:

```bash
zig build test
```

Expected before implementation: compile or test failures proving `.both` and runtime support are missing.

## Task 2: Implement protocol parsing and port conflict rules

**Files:**
- Modify: `src/model/allocation.zig`
- Modify: `src/service/allocation_service.zig`
- Optional modify: `src/storage/sqlite.zig`

- [ ] **Step 1: Add `.both` to protocol enum**

In `src/model/allocation.zig`:

```zig
pub const Protocol = enum {
    tcp,
    udp,
    both,

    pub fn fromString(text: []const u8) ?Protocol {
        if (std.ascii.eqlIgnoreCase(text, "tcp")) return .tcp;
        if (std.ascii.eqlIgnoreCase(text, "udp")) return .udp;
        if (std.ascii.eqlIgnoreCase(text, "both")) return .both;
        return null;
    }
```

- [ ] **Step 2: Replace exact-protocol conflict detection**

In `src/service/allocation_service.zig`, replace `exists()` with conflict-aware logic and call it inside the transaction that inserts the allocation. Runtime listener creation may happen before persistence, but the conflict check and insert must be one SQLite transaction so cross-protocol conflicts cannot slip between scan and insert:

```zig
fn conflicts(requested: model.Protocol, existing: model.Protocol) bool {
    return switch (requested) {
        .tcp => existing == .tcp or existing == .both,
        .udp => existing == .udp or existing == .both,
        .both => true,
    };
}

fn existsInCurrentTransaction(self: *Service, protocol: model.Protocol, port: u16) !bool {
    var allocations = try self.repo.listAllocations(self.allocator);
    defer {
        for (allocations.items) |*item| item.deinit(self.allocator);
        allocations.deinit(self.allocator);
    }
    for (allocations.items) |allocation| {
        if (allocation.port == port and conflicts(protocol, allocation.protocol)) return true;
    }
    return false;
}
```

The final `createAllocation()` control flow must not perform a separate pre-transaction conflict scan. It should begin a transaction for each candidate port, check `existsInCurrentTransaction()`, insert the allocation and optional binding, then commit; on runtime or insert failure, rollback and delete any runtime listeners created for that candidate.

- [ ] **Step 3: Check SQLite stable ordering**

Keep `ORDER BY a.protocol, a.port` unless tests require recommended order. Lexical order (`both`, `tcp`, `udp`) is allowed by the source proposal if documented.

- [ ] **Step 4: Run focused tests**

Run:

```bash
zig build test
```

Expected after this task: parser and conflict tests pass; runtime/HTTP tests may still fail until Task 3.

## Task 3: Implement dual listener runtime support

**Files:**
- Modify: `src/runtime/manager.zig`

- [ ] **Step 1: Add per-entry dual FD fields**

Extend `ListenerEntry` with explicit dual listener fields while preserving existing fields for single-protocol behavior:

```zig
tcp_fd: ?posix.fd_t,
udp_fd: ?posix.fd_t,
```

Initialize both to `null` in `createEntry()`.

- [ ] **Step 2: Add event dispatch protocol map**

Add to `RuntimeManager` and update handlers to accept concrete event FDs:

```zig
listener_fd_protocols: std.AutoHashMap(posix.fd_t, model.Protocol),
```

Initialize/deinit it next to `listener_fds`. When registering a listener FD, put both the entry and the concrete protocol into these maps. For existing single-protocol `entry.fd`, protocol is `entry.protocol`. For dual `tcp_fd`, protocol is `.tcp`; for dual `udp_fd`, protocol is `.udp`.

- [ ] **Step 3: Bind dual entries atomically**

Update `bindEntry()` or add `bindEntryListeners()` so `.both` binds TCP first and UDP second, rolling back TCP if UDP bind fails:

```zig
.both => {
    const tcp_fd = try bindTcpListener(entry.port, self.epoll_fd, false);
    errdefer closeIgnoreBadFd(tcp_fd);
    const udp_fd = try bindUdpSocket(entry.port, self.epoll_fd, self.udp_socket_recv_buffer_bytes, self.udp_socket_send_buffer_bytes, false, self.udp_gro_enabled and !self.udp_fast_path_test_force_fallback and self.udp_session_workers.len == 0);
    entry.tcp_fd = tcp_fd;
    entry.udp_fd = udp_fd;
}
```

Do not enable sharded TCP/UDP listener paths for `.both` in the first implementation; `useShardedTcpListeners()` and `useShardedUdpListeners()` should continue to return true only for single-protocol entries.

- [ ] **Step 4: Register and close all FDs**

Update `addToRegistry()` to register `entry.fd`, `entry.tcp_fd`, and `entry.udp_fd` with concrete protocol values. Update `closeEntryFd()` to remove and close all three optional FDs and clear protocol mappings.

- [ ] **Step 5: Dispatch by FD protocol**

In `handleFd()`, fetch the concrete protocol from `listener_fd_protocols` instead of switching on `entry.protocol`, and pass the event FD to protocol-specific handlers:

```zig
const concrete_protocol = self.listener_fd_protocols.get(fd) orelse entry.protocol;
switch (concrete_protocol) {
    .tcp => handleTcpAccept(self, entry, fd),
    .udp => handleUdpReadable(self.allocator, self.metrics, entry, fd),
    .both => unreachable,
}
```

- [ ] **Step 6: Update concrete-FD handler signatures**

Change these functions so dual UDP/TCP paths do not read `entry.fd`:

```zig
fn handleTcpAccept(manager: *RuntimeManager, entry: *ListenerEntry, listen_fd: posix.fd_t) void
fn handleUdpReadable(allocator: std.mem.Allocator, metrics: *Metrics, entry: *ListenerEntry, fd: posix.fd_t) void
fn handleUdpReadableGro(allocator: std.mem.Allocator, metrics: *Metrics, entry: *ListenerEntry, fd: posix.fd_t) void
fn armUdpIoUringListener(manager: *RuntimeManager, entry: *ListenerEntry, fd: posix.fd_t) void
```

Update `armUdpIoUringListeners()` to arm `.udp` entries with `entry.fd` and `.both` entries with `entry.udp_fd`. UDP reply dispatch can continue to carry `entry` because reply FDs are per upstream session, but initial listener reads must use the concrete UDP listener FD.

- [ ] **Step 7: Aggregate status stays one observed state**

Keep `snapshot()` returning `entry.observed()`. `update()` can keep one desired/effective binding because both listeners share one target. Update listener-unavailable checks to treat any present single or dual FD as available:

```zig
if (entry.fd == null and entry.tcp_fd == null and entry.udp_fd == null and entry.tcp_worker_listener_fds == null and entry.udp_worker_listener_fds == null) { ... }
```

- [ ] **Step 8: Run runtime tests**

Run:

```bash
zig build test
```

Expected: dual TCP/UDP forwarding, delete, and restore tests pass with existing TCP/UDP regression tests.

## Task 4: HTTP compatibility and documentation

**Files:**
- Modify: `src/http/server.zig` only if compile errors remain for exhaustive protocol switches.
- Modify: `docs/API.md`
- Modify: `docs/api/http.md`

- [ ] **Step 1: Fix HTTP compile/runtime gaps**

Search for protocol switches or validation that assume only `tcp`/`udp`:

```bash
grep -R "switch (.*protocol\|\.tcp\|\.udp\|invalid protocol" -n src/http src/service src/storage src/model
```

Add `.both` handling only where the compiler or tests require it. Existing `model.Protocol.fromString()` should make create endpoints accept `both` without route changes.

- [ ] **Step 2: Update `docs/API.md`**

Document:

- `protocol` enum is `tcp | udp | both`.
- `both` reserves one relay port for both TCP and UDP.
- Binding target is shared by both protocols.
- `/v1/ports` returns one aggregate row for `both`.
- No available shared port maps to `409`.

- [ ] **Step 3: Update `docs/api/http.md`**

Mirror the endpoint contract and examples for `POST /v1/allocations`, `GET /v1/allocations`, `GET /v1/ports`, and compatibility `POST /v1/ports`.

- [ ] **Step 4: Run docs-sensitive grep**

Run:

```bash
grep -R "tcp | udp\|tcp.*udp\|protocol" -n docs/API.md docs/api/http.md
```

Expected: docs consistently mention `both` where protocol values are listed.

## Task 5: Final verification and review gates

**Files:**
- No required file edits unless review finds gaps.

- [ ] **Step 1: Full verification**

Run:

```bash
zig build test
```

Expected: all tests pass.

- [ ] **Step 2: Build prompt-to-artifact checklist**

Map every acceptance criterion in this plan to concrete test names, code paths, docs sections, and command output.

- [ ] **Step 3: Independent implementation review**

Dispatch an independent reviewer with `dual-protocol-port-allocation.md`, the spec, this plan, and the diff. The reviewer must answer `APPROVE` or list required fixes. Do not mark implementation complete until the reviewer returns `APPROVE`.

- [ ] **Step 4: Apply fixes and re-review until APPROVE**

If review finds issues, fix them, rerun relevant tests, then re-dispatch review. Repeat until `APPROVE`.

- [ ] **Step 5: Final docs update, commit, and push**

After implementation APPROVE, make any final docs adjustments, rerun verification, then commit and push using the Lore commit protocol.

Suggested commit intent:

```text
Allow one allocation to reserve both relay protocols

Constraint: API proposal requires one logical allocation to own TCP and UDP listeners on the same port.
Confidence: high
Scope-risk: moderate
Directive: Keep both-protocol binding shared unless a future API introduces per-protocol binding shape.
Tested: zig build test
Not-tested: Terraform provider validation change; provider lives outside this repository.
```
