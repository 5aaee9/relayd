#!/usr/bin/env bash
set -euo pipefail

AUTH_TOKEN="${AUTH_TOKEN:-test-token}"
HTTP_LISTEN="${HTTP_LISTEN:-127.0.0.1:18080}"
PORT_RANGE="${PORT_RANGE:-18100-18120}"
SQLITE_PATH="${SQLITE_PATH:-.zig-cache/e2e/relayd-$$.sqlite3}"
TARGET_HOST="${TARGET_HOST:-127.0.0.1}"
UDP_RATE="${UDP_RATE:-1M}"
IPERF_DURATION="${IPERF_DURATION:-2}"
READINESS_TIMEOUT_SEC="${READINESS_TIMEOUT_SEC:-30}"
ACTIVE_TIMEOUT_SEC="${ACTIVE_TIMEOUT_SEC:-30}"
RELAYD_BIN="zig-out/bin/relayd"

if [[ "$HTTP_LISTEN" == :* ]]; then
  API_HTTP_LISTEN="127.0.0.1${HTTP_LISTEN}"
else
  API_HTTP_LISTEN="$HTTP_LISTEN"
fi

ARTIFACT_DIR=".zig-cache/e2e"
RUN_DIR="${ARTIFACT_DIR}/iperf3-$$"
RELAYD_LOG="${RUN_DIR}/relayd.log"
TCP_SERVER_LOG="${RUN_DIR}/iperf3-tcp-server.log"
TCP_CLIENT_JSON="${RUN_DIR}/iperf3-tcp-client.json"
TCP_CLIENT_LOG="${RUN_DIR}/iperf3-tcp-client.log"
UDP_SERVER_LOG="${RUN_DIR}/iperf3-udp-server.log"
UDP_CLIENT_JSON="${RUN_DIR}/iperf3-udp-client.json"
UDP_CLIENT_LOG="${RUN_DIR}/iperf3-udp-client.log"
LATEST_LIST_JSON="${RUN_DIR}/ports-list.json"
LATEST_LIST_STATUS="${RUN_DIR}/ports-list.status"

child_pids=()
allocation_ids=()
cleanup_running=0
CREATED_ALLOCATION_ID=
CREATED_RELAY_PORT=

log() {
  printf '[e2e_iperf3] %s\n' "$*" >&2
}

die() {
  log "ERROR: $*"
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

track_child() {
  child_pids+=("$1")
}

dump_file() {
  local label=$1
  local path=$2
  if [[ -f "$path" ]]; then
    log "--- ${label}: ${path} ---"
    sed 's/^/    /' "$path" >&2 || true
  fi
}

dump_logs() {
  dump_file relayd "$RELAYD_LOG"
  dump_file tcp_server "$TCP_SERVER_LOG"
  dump_file tcp_client "$TCP_CLIENT_LOG"
  dump_file tcp_client_json "$TCP_CLIENT_JSON"
  dump_file udp_server "$UDP_SERVER_LOG"
  dump_file udp_client "$UDP_CLIENT_LOG"
  dump_file udp_client_json "$UDP_CLIENT_JSON"
  dump_file ports_list_status "$LATEST_LIST_STATUS"
  dump_file ports_list_json "$LATEST_LIST_JSON"
}

api_request() {
  local status_file=$1
  local body_file=$2
  local method=$3
  local path=$4
  local token=$5
  local body=${6-}

  HTTP_STATUS_FILE="$status_file" \
  HTTP_BODY_FILE="$body_file" \
  HTTP_METHOD="$method" \
  HTTP_URL="http://${API_HTTP_LISTEN}${path}" \
  HTTP_BEARER_TOKEN="$token" \
  HTTP_BODY_PAYLOAD="$body" \
  python3 <<'PY'
import os
import sys
import urllib.error
import urllib.request

status_file = os.environ["HTTP_STATUS_FILE"]
body_file = os.environ["HTTP_BODY_FILE"]
method = os.environ["HTTP_METHOD"]
url = os.environ["HTTP_URL"]
token = os.environ["HTTP_BEARER_TOKEN"]
body = os.environ.get("HTTP_BODY_PAYLOAD", "")
headers = {"Authorization": f"Bearer {token}"}
data = None
if method in {"POST", "PUT", "PATCH"}:
    headers["Content-Type"] = "application/json"
    data = body.encode()
request = urllib.request.Request(url, data=data, method=method, headers=headers)
try:
    with urllib.request.urlopen(request, timeout=5) as response:
        status = response.getcode()
        payload = response.read().decode()
except urllib.error.HTTPError as err:
    status = err.code
    payload = err.read().decode()
except urllib.error.URLError:
    raise SystemExit(75)
with open(status_file, "w", encoding="utf-8") as handle:
    handle.write(str(status))
with open(body_file, "w", encoding="utf-8") as handle:
    handle.write(payload)
PY
}

json_get() {
  local json_file=$1
  local expression=$2
  python3 - "$json_file" "$expression" <<'PY'
import json
import sys

path = sys.argv[2].split('.')
value = json.load(open(sys.argv[1], encoding='utf-8'))
for key in path:
    if isinstance(value, list):
        value = value[int(key)]
    else:
        value = value[key]
if value is None:
    sys.exit(1)
if isinstance(value, bool):
    print("true" if value else "false")
elif isinstance(value, (int, float)):
    print(value)
else:
    print(value)
PY
}

allocation_field_from_list() {
  local json_file=$1
  local allocation_id=$2
  local field=$3
  python3 - "$json_file" "$allocation_id" "$field" <<'PY'
import json
import sys

items = json.load(open(sys.argv[1], encoding='utf-8'))
needle = sys.argv[2]
field = sys.argv[3]
for item in items:
    if item.get("id") == needle:
        value = item.get(field)
        if value is None:
            sys.exit(1)
        if isinstance(value, bool):
            print("true" if value else "false")
        else:
            print(value)
        sys.exit(0)
sys.exit(2)
PY
}

assert_iperf_positive() {
  local json_file=$1
  local mode=$2
  python3 - "$json_file" "$mode" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding='utf-8'))
mode = sys.argv[2]
end = payload.get("end") or {}
if mode == "tcp":
    summary = end.get("sum_received") or end.get("sum") or {}
else:
    summary = end.get("sum") or {}
byte_count = summary.get("bytes") or 0
bitrate = summary.get("bits_per_second") or 0
if byte_count <= 0 or bitrate <= 0:
    raise SystemExit(f"invalid iperf3 {mode} summary: bytes={byte_count} bits_per_second={bitrate}")
print(f"bytes={byte_count} bits_per_second={bitrate}")
PY
}

udp_lost_percent() {
  local json_file=$1
  python3 - "$json_file" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding='utf-8'))
summary = (payload.get("end") or {}).get("sum") or {}
lost_percent = summary.get("lost_percent")
if lost_percent is not None:
    print(lost_percent)
PY
}

pick_free_port() {
  local protocol=${1:-tcp}
  python3 - "$protocol" <<'PY'
import socket
import sys

protocol = sys.argv[1]
sock_type = socket.SOCK_DGRAM if protocol == "udp" else socket.SOCK_STREAM
sock = socket.socket(socket.AF_INET, sock_type)
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
}

wait_for_status() {
  local expected=$1
  local token=$2
  local path=$3
  local timeout_sec=$4
  local body_file=$5
  local status_file=$6

  local attempt
  for ((attempt = 1; attempt <= timeout_sec; attempt++)); do
    if api_request "$status_file" "$body_file" GET "$path" "$token"; then
      local status
      status=$(<"$status_file")
      if [[ "$status" == "$expected" ]]; then
        return 0
      fi
    fi
    sleep 1
  done

  return 1
}

wait_for_active() {
  local allocation_id=$1
  local attempt
  for ((attempt = 1; attempt <= ACTIVE_TIMEOUT_SEC; attempt++)); do
    api_request "$LATEST_LIST_STATUS" "$LATEST_LIST_JSON" GET /v1/ports "$AUTH_TOKEN"
    local status
    status=$(<"$LATEST_LIST_STATUS")
    [[ "$status" == "200" ]] || {
      sleep 1
      continue
    }

    local runtime_status
    if runtime_status=$(allocation_field_from_list "$LATEST_LIST_JSON" "$allocation_id" runtime_status 2>/dev/null); then
      if [[ "$runtime_status" == "active" ]]; then
        return 0
      fi
      log "allocation ${allocation_id} runtime_status=${runtime_status} (waiting for active)"
    fi
    sleep 1
  done

  return 1
}

delete_allocation() {
  local allocation_id=$1
  local delete_status="${RUN_DIR}/delete-now-${allocation_id}.status"
  local delete_body="${RUN_DIR}/delete-now-${allocation_id}.body"

  api_request "$delete_status" "$delete_body" DELETE "/v1/ports/${allocation_id}" "$AUTH_TOKEN" || true
  local status
  status=$(<"$delete_status")
  if [[ "$status" != "204" && "$status" != "404" ]]; then
    die "allocation delete failed for ${allocation_id} with status ${status}"
  fi
}

create_allocation() {
  local protocol=$1
  local target_port=$2
  local create_status="${RUN_DIR}/${protocol}-create.status"
  local create_body="${RUN_DIR}/${protocol}-create.json"
  local update_status="${RUN_DIR}/${protocol}-update.status"
  local update_body="${RUN_DIR}/${protocol}-update.json"

  api_request "$create_status" "$create_body" POST /v1/ports "$AUTH_TOKEN" "{\"protocol\":\"${protocol}\",\"target_port\":${target_port}}"
  [[ $(<"$create_status") == "201" ]] || die "${protocol} allocation create failed with status $(<"$create_status")"

  local allocation_id relay_port
  allocation_id=$(json_get "$create_body" id)
  relay_port=$(json_get "$create_body" port)
  allocation_ids+=("$allocation_id")

  api_request "$update_status" "$update_body" POST /v1/ports/target "$AUTH_TOKEN" "{\"id\":\"${allocation_id}\",\"host\":\"${TARGET_HOST}\"}"
  [[ $(<"$update_status") == "200" ]] || die "${protocol} allocation host update failed with status $(<"$update_status")"

  if ! wait_for_active "$allocation_id"; then
    die "${protocol} allocation ${allocation_id} did not become active"
  fi

  CREATED_ALLOCATION_ID=$allocation_id
  CREATED_RELAY_PORT=$relay_port
}

cleanup() {
  local rc=$?
  if (( cleanup_running )); then
    exit "$rc"
  fi
  cleanup_running=1
  trap - EXIT INT TERM

  local pid allocation_id
  for allocation_id in "${allocation_ids[@]:-}"; do
    if [[ -n "${allocation_id}" ]] && [[ -f "$RELAYD_LOG" ]] && kill -0 "${child_pids[0]:-0}" 2>/dev/null; then
      api_request "${RUN_DIR}/delete-${allocation_id}.status" "${RUN_DIR}/delete-${allocation_id}.body" DELETE "/v1/ports/${allocation_id}" "$AUTH_TOKEN" || true
    fi
  done

  for pid in "${child_pids[@]:-}"; do
    [[ -n "$pid" ]] || continue
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done

  for pid in "${child_pids[@]:-}"; do
    [[ -n "$pid" ]] || continue
    wait "$pid" 2>/dev/null || true
  done

  local cleanup_failed=0
  for pid in "${child_pids[@]:-}"; do
    [[ -n "$pid" ]] || continue
    if kill -0 "$pid" 2>/dev/null; then
      log "tracked child pid still alive after cleanup: $pid"
      cleanup_failed=1
    fi
  done

  if (( rc != 0 || cleanup_failed != 0 )); then
    dump_logs
  else
    rm -f "$SQLITE_PATH"
    rm -rf "$RUN_DIR"
  fi

  if (( cleanup_failed != 0 && rc == 0 )); then
    rc=1
  fi

  exit "$rc"
}
trap cleanup EXIT INT TERM

mkdir -p "$ARTIFACT_DIR" "$RUN_DIR"
mkdir -p "$(dirname "$SQLITE_PATH")"

require_cmd python3
require_cmd iperf3
[[ -x "$RELAYD_BIN" ]] || die "expected relayd build artifact at ${RELAYD_BIN}"

log "starting relayd on ${HTTP_LISTEN} with port range ${PORT_RANGE}"
AUTH_TOKEN="$AUTH_TOKEN" \
HTTP_LISTEN="$HTTP_LISTEN" \
PORT_RANGE="$PORT_RANGE" \
SQLITE_PATH="$SQLITE_PATH" \
"$RELAYD_BIN" >"$RELAYD_LOG" 2>&1 &
relayd_pid=$!
track_child "$relayd_pid"

readiness_body="${RUN_DIR}/readiness.body"
readiness_status="${RUN_DIR}/readiness.status"
if ! wait_for_status 200 "$AUTH_TOKEN" /v1/ports "$READINESS_TIMEOUT_SEC" "$readiness_body" "$readiness_status"; then
  die "authenticated readiness probe did not return 200"
fi
log "authenticated readiness probe returned 200"

unauth_body="${RUN_DIR}/unauth.body"
unauth_status="${RUN_DIR}/unauth.status"
api_request "$unauth_status" "$unauth_body" GET /v1/ports bad-token
[[ $(<"$unauth_status") == "401" ]] || die "bad token probe expected 401, got $(<"$unauth_status")"
log "bad token probe returned 401"

tcp_target_port=$(pick_free_port tcp)
log "starting tcp iperf3 server on ${TARGET_HOST}:${tcp_target_port}"
iperf3 -s -1 -B "$TARGET_HOST" -p "$tcp_target_port" >"$TCP_SERVER_LOG" 2>&1 &
tcp_server_pid=$!
track_child "$tcp_server_pid"
create_allocation tcp "$tcp_target_port"
tcp_allocation_id=$CREATED_ALLOCATION_ID
tcp_relay_port=$CREATED_RELAY_PORT
log "tcp allocation id=${tcp_allocation_id} relay_port=${tcp_relay_port}"
iperf3 -c "$TARGET_HOST" -p "$tcp_relay_port" -t "$IPERF_DURATION" -J >"$TCP_CLIENT_JSON" 2>"$TCP_CLIENT_LOG"
log "tcp $(assert_iperf_positive "$TCP_CLIENT_JSON" tcp)"
wait "$tcp_server_pid"
delete_allocation "$tcp_allocation_id"

udp_target_port=$(pick_free_port tcp)
log "starting udp iperf3 server on ${TARGET_HOST}:${udp_target_port} (requires tcp control + udp data on the same relay port)"
iperf3 -s -1 -B "$TARGET_HOST" -p "$udp_target_port" >"$UDP_SERVER_LOG" 2>&1 &
udp_server_pid=$!
track_child "$udp_server_pid"
create_allocation tcp "$udp_target_port"
udp_control_allocation_id=$CREATED_ALLOCATION_ID
udp_control_relay_port=$CREATED_RELAY_PORT
log "udp control allocation id=${udp_control_allocation_id} relay_port=${udp_control_relay_port}"
create_allocation udp "$udp_target_port"
udp_allocation_id=$CREATED_ALLOCATION_ID
udp_relay_port=$CREATED_RELAY_PORT
log "udp allocation id=${udp_allocation_id} relay_port=${udp_relay_port}"
[[ "$udp_control_relay_port" == "$udp_relay_port" ]] || die "iperf3 UDP requires matching TCP control and UDP relay ports, got tcp=${udp_control_relay_port} udp=${udp_relay_port}"
udp_metrics=$(iperf3 -c "$TARGET_HOST" -p "$udp_relay_port" -u -b "$UDP_RATE" -t "$IPERF_DURATION" -J >"$UDP_CLIENT_JSON" 2>"$UDP_CLIENT_LOG"; assert_iperf_positive "$UDP_CLIENT_JSON" udp)
log "udp ${udp_metrics}"
if udp_loss=$(udp_lost_percent "$UDP_CLIENT_JSON"); then
  [[ -n "$udp_loss" ]] && log "udp lost_percent=${udp_loss} (non-gating)"
fi
wait "$udp_server_pid"

log "iperf3 TCP+UDP e2e coverage passed"
