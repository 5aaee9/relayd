#!/usr/bin/env bash
set -euo pipefail

AUTH_TOKEN="${AUTH_TOKEN:-test-token}"
HTTP_LISTEN="${HTTP_LISTEN:-127.0.0.1:18080}"
PORT_RANGE="${PORT_RANGE:-18100-18120}"
SQLITE_PATH="${SQLITE_PATH:-.zig-cache/e2e/relayd-$$.sqlite3}"
TARGET_HOST="${TARGET_HOST:-127.0.0.1}"
TCP_SESSION_MODEL_ENABLED="${TCP_SESSION_MODEL_ENABLED:-0}"
TCP_SESSION_MODEL_WORKERS="${TCP_SESSION_MODEL_WORKERS:-0}"
TCP_SESSION_MODEL_ACCEPT_BALANCED="${TCP_SESSION_MODEL_ACCEPT_BALANCED:-0}"
TCP_SESSION_MODEL_SHARDED_ACCEPT="${TCP_SESSION_MODEL_SHARDED_ACCEPT:-0}"
TCP_SPLICE_ENABLED="${TCP_SPLICE_ENABLED:-0}"
FORCE_TCP_COPY_FALLBACK="${FORCE_TCP_COPY_FALLBACK:-0}"
UDP_RATE="${UDP_RATE:-100G}"
UDP_PACKET_SIZE="${UDP_PACKET_SIZE:-}"
IPERF_DURATION="${IPERF_DURATION:-2}"
IPERF_MODE="${IPERF_MODE:-oneshot}"
UDP_SWEEP_RATES="${UDP_SWEEP_RATES:-1G,5G,10G,25G,50G,100G}"
UDP_PACKET_SIZES="${UDP_PACKET_SIZES:-256,1200,1472}"
IPERF_REPETITIONS="${IPERF_REPETITIONS:-3}"
UDP_MATRIX_DURATION="${UDP_MATRIX_DURATION:-$IPERF_DURATION}"
TCP_DIRECT_VS_RELAY="${TCP_DIRECT_VS_RELAY:-1}"
TCP_COMPARE_MODE="${TCP_COMPARE_MODE:-accept-balanced}"
TCP_BENCH_DURATION="${TCP_BENCH_DURATION:-$IPERF_DURATION}"
TCP_BENCH_REPETITIONS="${TCP_BENCH_REPETITIONS:-$IPERF_REPETITIONS}"
TCP_STREAMS="${TCP_STREAMS:-1}"
IPERF_KEEP_RUN_DIR="${IPERF_KEEP_RUN_DIR:-0}"
IPERF_SERVER_READY_TIMEOUT_SEC="${IPERF_SERVER_READY_TIMEOUT_SEC:-5}"
READINESS_TIMEOUT_SEC="${READINESS_TIMEOUT_SEC:-30}"
ACTIVE_TIMEOUT_SEC="${ACTIVE_TIMEOUT_SEC:-30}"
RELAYD_BIN="zig-out/bin/relayd"
IPERF3_BIN="${IPERF3_BIN:-iperf3}"
if [[ "$IPERF3_BIN" == "iperf3" ]] && ! command -v iperf3 >/dev/null 2>&1 && [[ -x ".zig-cache/tools/iperf-3.20/build/bin/iperf3" ]]; then
  IPERF3_BIN=".zig-cache/tools/iperf-3.20/build/bin/iperf3"
fi

if [[ "$HTTP_LISTEN" == :* ]]; then
  API_HTTP_LISTEN="127.0.0.1${HTTP_LISTEN}"
else
  API_HTTP_LISTEN="$HTTP_LISTEN"
fi

ARTIFACT_DIR=".zig-cache/e2e"
RUN_DIR="${ARTIFACT_DIR}/iperf3-$$"
LATEST_RUN_DIR="${ARTIFACT_DIR}/iperf3-latest"
RELAYD_LOG="${RUN_DIR}/relayd.log"
TCP_SERVER_LOG="${RUN_DIR}/iperf3-tcp-server.log"
TCP_CLIENT_JSON="${RUN_DIR}/iperf3-tcp-client.json"
TCP_CLIENT_LOG="${RUN_DIR}/iperf3-tcp-client.log"
TCP_COPY_VS_SPLICE_DIR="${RUN_DIR}/tcp-copy-vs-splice/streams-${TCP_STREAMS}"
TCP_COPY_VS_SPLICE_RESULTS="${RUN_DIR}/tcp-copy-vs-splice-streams-${TCP_STREAMS}-results.ndjson"
TCP_COPY_VS_SPLICE_SUMMARY_TXT="${RUN_DIR}/tcp-copy-vs-splice-streams-${TCP_STREAMS}-summary.txt"
TCP_COPY_VS_SPLICE_SUMMARY_JSON="${RUN_DIR}/tcp-copy-vs-splice-streams-${TCP_STREAMS}-summary.json"
TCP_COPY_VS_SPLICE_OVERALL_SUMMARY="${RUN_DIR}/tcp-copy-vs-splice-overall-summary.txt"
TCP_SESSION_MODEL_DIR="${RUN_DIR}/tcp-session-model/streams-${TCP_STREAMS}"
TCP_SESSION_MODEL_RESULTS="${RUN_DIR}/tcp-session-model-streams-${TCP_STREAMS}-results.ndjson"
TCP_SESSION_MODEL_SUMMARY_TXT="${RUN_DIR}/tcp-session-model-streams-${TCP_STREAMS}-summary.txt"
TCP_SESSION_MODEL_SUMMARY_JSON="${RUN_DIR}/tcp-session-model-streams-${TCP_STREAMS}-summary.json"
TCP_SESSION_MODEL_OVERALL_SUMMARY="${RUN_DIR}/tcp-session-model-overall-summary.txt"
TCP_WORKERIZED_SESSION_DIR="${RUN_DIR}/tcp-workerized-session/streams-${TCP_STREAMS}"
TCP_WORKERIZED_SESSION_RESULTS="${RUN_DIR}/tcp-workerized-session-streams-${TCP_STREAMS}-results.ndjson"
TCP_WORKERIZED_SESSION_SUMMARY_TXT="${RUN_DIR}/tcp-workerized-session-streams-${TCP_STREAMS}-summary.txt"
TCP_WORKERIZED_SESSION_SUMMARY_JSON="${RUN_DIR}/tcp-workerized-session-streams-${TCP_STREAMS}-summary.json"
TCP_WORKERIZED_SESSION_OVERALL_SUMMARY="${RUN_DIR}/tcp-workerized-session-overall-summary.txt"
TCP_SHARDED_WORKER_DIR="${RUN_DIR}/tcp-sharded-worker/streams-${TCP_STREAMS}"
TCP_SHARDED_WORKER_RESULTS="${RUN_DIR}/tcp-sharded-worker-streams-${TCP_STREAMS}-results.ndjson"
TCP_SHARDED_WORKER_SUMMARY_TXT="${RUN_DIR}/tcp-sharded-worker-streams-${TCP_STREAMS}-summary.txt"
TCP_SHARDED_WORKER_SUMMARY_JSON="${RUN_DIR}/tcp-sharded-worker-streams-${TCP_STREAMS}-summary.json"
TCP_SHARDED_WORKER_OVERALL_SUMMARY="${RUN_DIR}/tcp-sharded-worker-overall-summary.txt"
TCP_ACCEPT_BALANCED_DIR="${RUN_DIR}/tcp-accept-balanced/streams-${TCP_STREAMS}"
TCP_ACCEPT_BALANCED_RESULTS="${RUN_DIR}/tcp-accept-balanced-streams-${TCP_STREAMS}-results.ndjson"
TCP_ACCEPT_BALANCED_SUMMARY_TXT="${RUN_DIR}/tcp-accept-balanced-streams-${TCP_STREAMS}-summary.txt"
TCP_ACCEPT_BALANCED_SUMMARY_JSON="${RUN_DIR}/tcp-accept-balanced-streams-${TCP_STREAMS}-summary.json"
TCP_ACCEPT_BALANCED_OVERALL_SUMMARY="${RUN_DIR}/tcp-accept-balanced-overall-summary.txt"
TCP_DIRECT_JSON="${RUN_DIR}/tcp-direct-client.json"
TCP_DIRECT_LOG="${RUN_DIR}/tcp-direct-client.log"
TCP_DIRECT_SERVER_LOG="${RUN_DIR}/tcp-direct-server.log"
TCP_COPY_JSON="${RUN_DIR}/tcp-copy-client.json"
TCP_COPY_LOG="${RUN_DIR}/tcp-copy-client.log"
TCP_COPY_SERVER_LOG="${RUN_DIR}/tcp-copy-server.log"
TCP_SPLICE_JSON="${RUN_DIR}/tcp-splice-client.json"
TCP_SPLICE_LOG="${RUN_DIR}/tcp-splice-client.log"
TCP_SPLICE_SERVER_LOG="${RUN_DIR}/tcp-splice-server.log"
TCP_THREADED_JSON="${RUN_DIR}/tcp-threaded-client.json"
TCP_THREADED_LOG="${RUN_DIR}/tcp-threaded-client.log"
TCP_THREADED_SERVER_LOG="${RUN_DIR}/tcp-threaded-server.log"
TCP_SESSION_MODEL_JSON="${RUN_DIR}/tcp-session-model-client.json"
TCP_SESSION_MODEL_LOG="${RUN_DIR}/tcp-session-model-client.log"
TCP_SESSION_MODEL_SERVER_LOG="${RUN_DIR}/tcp-session-model-server.log"
TCP_WORKERIZED_SESSION_JSON="${RUN_DIR}/tcp-workerized-session-client.json"
TCP_WORKERIZED_SESSION_LOG="${RUN_DIR}/tcp-workerized-session-client.log"
TCP_WORKERIZED_SESSION_SERVER_LOG="${RUN_DIR}/tcp-workerized-session-server.log"
TCP_SHARDED_WORKER_JSON="${RUN_DIR}/tcp-sharded-worker-client.json"
TCP_SHARDED_WORKER_LOG="${RUN_DIR}/tcp-sharded-worker-client.log"
TCP_SHARDED_WORKER_SERVER_LOG="${RUN_DIR}/tcp-sharded-worker-server.log"
TCP_ACCEPT_BALANCED_JSON="${RUN_DIR}/tcp-accept-balanced-client.json"
TCP_ACCEPT_BALANCED_LOG="${RUN_DIR}/tcp-accept-balanced-client.log"
TCP_ACCEPT_BALANCED_SERVER_LOG="${RUN_DIR}/tcp-accept-balanced-server.log"
UDP_SERVER_LOG="${RUN_DIR}/iperf3-udp-server.log"
UDP_CLIENT_JSON="${RUN_DIR}/iperf3-udp-client.json"
UDP_CLIENT_LOG="${RUN_DIR}/iperf3-udp-client.log"
LATEST_LIST_JSON="${RUN_DIR}/ports-list.json"
LATEST_LIST_STATUS="${RUN_DIR}/ports-list.status"
ONE_SHOT_REPORT="${RUN_DIR}/one-shot-report.txt"
UDP_MATRIX_RESULTS="${RUN_DIR}/udp-matrix-results.ndjson"
UDP_MATRIX_SUMMARY_TXT="${RUN_DIR}/udp-matrix-summary.txt"
UDP_MATRIX_SUMMARY_JSON="${RUN_DIR}/udp-matrix-summary.json"
RUN_MANIFEST="${RUN_DIR}/run-manifest.txt"

child_pids=()
allocation_ids=()
cleanup_running=0
CREATED_ALLOCATION_ID=
CREATED_RELAY_PORT=
RELAYD_PID=

log() {
  printf '[e2e_iperf3] %s\n' "$*" >&2
}

die() {
  log "ERROR: $*"
  exit 1
}

require_cmd() {
  if [[ "$1" == */* ]]; then
    [[ -x "$1" ]] || die "required command not found: $1"
    return
  fi
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

track_child() {
  child_pids+=("$1")
}

split_csv() {
  local -n out=$1
  local csv=$2
  local item
  IFS=',' read -r -a out <<<"$csv"
  for item in "${!out[@]}"; do
    out[$item]="${out[$item]// /}"
    [[ -n "${out[$item]}" ]] || die "empty item in CSV list: ${csv}"
  done
}

validate_mode() {
  case "$IPERF_MODE" in
    oneshot|matrix|both) ;;
    *) die "IPERF_MODE must be one of: oneshot, matrix, both (got ${IPERF_MODE})" ;;
  esac
  case "$TCP_COMPARE_MODE" in
    accept-balanced|sharded-worker|workerized-session|session-model|copy-vs-splice) ;;
    *) die "TCP_COMPARE_MODE must be one of: accept-balanced, sharded-worker, workerized-session, session-model, copy-vs-splice (got ${TCP_COMPARE_MODE})" ;;
  esac
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
  dump_file tcp_copy_vs_splice_summary "$TCP_COPY_VS_SPLICE_SUMMARY_TXT"
  dump_file tcp_copy_vs_splice_overall "$TCP_COPY_VS_SPLICE_OVERALL_SUMMARY"
  dump_file tcp_session_model_summary "$TCP_SESSION_MODEL_SUMMARY_TXT"
  dump_file tcp_session_model_overall "$TCP_SESSION_MODEL_OVERALL_SUMMARY"
  dump_file tcp_workerized_session_summary "$TCP_WORKERIZED_SESSION_SUMMARY_TXT"
  dump_file tcp_workerized_session_overall "$TCP_WORKERIZED_SESSION_OVERALL_SUMMARY"
  dump_file tcp_sharded_worker_summary "$TCP_SHARDED_WORKER_SUMMARY_TXT"
  dump_file tcp_sharded_worker_overall "$TCP_SHARDED_WORKER_OVERALL_SUMMARY"
  dump_file tcp_accept_balanced_summary "$TCP_ACCEPT_BALANCED_SUMMARY_TXT"
  dump_file tcp_accept_balanced_overall "$TCP_ACCEPT_BALANCED_OVERALL_SUMMARY"
  dump_file tcp_direct_client "$TCP_DIRECT_LOG"
  dump_file tcp_direct_client_json "$TCP_DIRECT_JSON"
  dump_file tcp_copy_client "$TCP_COPY_LOG"
  dump_file tcp_copy_client_json "$TCP_COPY_JSON"
  dump_file tcp_splice_client "$TCP_SPLICE_LOG"
  dump_file tcp_splice_client_json "$TCP_SPLICE_JSON"
  dump_file tcp_threaded_client "$TCP_THREADED_LOG"
  dump_file tcp_threaded_client_json "$TCP_THREADED_JSON"
  dump_file tcp_session_model_client "$TCP_SESSION_MODEL_LOG"
  dump_file tcp_session_model_client_json "$TCP_SESSION_MODEL_JSON"
  dump_file tcp_workerized_session_client "$TCP_WORKERIZED_SESSION_LOG"
  dump_file tcp_workerized_session_client_json "$TCP_WORKERIZED_SESSION_JSON"
  dump_file tcp_sharded_worker_client "$TCP_SHARDED_WORKER_LOG"
  dump_file tcp_sharded_worker_client_json "$TCP_SHARDED_WORKER_JSON"
  dump_file tcp_accept_balanced_client "$TCP_ACCEPT_BALANCED_LOG"
  dump_file tcp_accept_balanced_client_json "$TCP_ACCEPT_BALANCED_JSON"
  dump_file udp_server "$UDP_SERVER_LOG"
  dump_file udp_client "$UDP_CLIENT_LOG"
  dump_file udp_client_json "$UDP_CLIENT_JSON"
  dump_file ports_list_status "$LATEST_LIST_STATUS"
  dump_file ports_list_json "$LATEST_LIST_JSON"
  dump_file one_shot_report "$ONE_SHOT_REPORT"
  dump_file udp_matrix_summary "$UDP_MATRIX_SUMMARY_TXT"
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

emit_stdout_report() {
  local tcp_json=$1
  local udp_json=$2
  local udp_target_rate=$3
  local udp_packet_size=$4
  python3 - "$tcp_json" "$udp_json" "$udp_target_rate" "$udp_packet_size" <<'PY'
import json
import sys

def format_decimal(value, units):
    value = float(value)
    unit_index = 0
    while value >= 1000.0 and unit_index < len(units) - 1:
        value /= 1000.0
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"

def tcp_summary(path):
    payload = json.load(open(path, encoding="utf-8"))
    end = payload.get("end") or {}
    summary = end.get("sum_received") or end.get("sum") or {}
    return {
        "bytes": summary.get("bytes", 0),
        "bps": summary.get("bits_per_second", 0.0),
    }

def udp_summary(path):
    payload = json.load(open(path, encoding="utf-8"))
    end = payload.get("end") or {}
    summary = end.get("sum_received") or end.get("sum") or {}
    return {
        "bytes": summary.get("bytes", 0),
        "bps": summary.get("bits_per_second", 0.0),
        "jitter_ms": summary.get("jitter_ms", 0.0),
        "lost_packets": summary.get("lost_packets", 0),
        "packets": summary.get("packets", 0),
        "lost_percent": summary.get("lost_percent", 0.0),
    }

tcp = tcp_summary(sys.argv[1])
udp = udp_summary(sys.argv[2])
udp_target = sys.argv[3]
udp_packet_size = sys.argv[4]

print("=== relayd e2e iperf3 report ===")
print(f"TCP throughput: {format_decimal(tcp['bps'], ['bps', 'kbps', 'mbps', 'gbps'])}")
print(f"TCP transfer:   {format_decimal(tcp['bytes'], ['bytes', 'kbytes', 'mbytes', 'gbytes'])}")
print(f"UDP target:     {udp_target}")
if udp_packet_size:
    print(f"UDP payload:    {udp_packet_size} bytes")
print(f"UDP throughput: {format_decimal(udp['bps'], ['bps', 'kbps', 'mbps', 'gbps'])}")
print(f"UDP transfer:   {format_decimal(udp['bytes'], ['bytes', 'kbytes', 'mbytes', 'gbytes'])}")
print(f"UDP loss:       {udp['lost_packets']}/{udp['packets']} packets ({udp['lost_percent']:.2f}%)")
print(f"UDP jitter:     {udp['jitter_ms']:.3f} ms")
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

wait_for_tcp_listener() {
  local host=$1
  local port=$2
  local timeout_sec=$3
  python3 - "$host" "$port" "$timeout_sec" <<'PY'
import sys
import time

_host = sys.argv[1]
port = int(sys.argv[2])
timeout = float(sys.argv[3])
listen_port = f"{port:04X}"
deadline = time.time() + timeout

def has_listener() -> bool:
    for table in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(table, encoding="utf-8") as handle:
                next(handle, None)
                for line in handle:
                    fields = line.split()
                    if len(fields) < 4:
                        continue
                    local_address = fields[1]
                    state = fields[3]
                    if state != "0A":
                        continue
                    _, local_port = local_address.rsplit(":", 1)
                    if local_port.upper() == listen_port:
                        return True
        except FileNotFoundError:
            continue
    return False

while time.time() < deadline:
    if has_listener():
        raise SystemExit(0)
    time.sleep(0.05)
raise SystemExit(1)
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

start_relayd() {
  local tcp_splice_enabled=$1
  local force_tcp_copy_fallback=$2
  local mode_label=$3
  local tcp_session_model_enabled=${4:-0}
  local tcp_session_model_workers=${5:-0}
  local tcp_session_model_sharded_accept=${6:-0}
  local tcp_session_model_accept_balanced=${7:-0}
  local sqlite_path="${RUN_DIR}/relayd-${mode_label}.sqlite3"

  stop_relayd
  rm -f "$sqlite_path"
  : >"${RUN_DIR}/readiness.status"
  : >"${RUN_DIR}/readiness.body"
  : >"${RUN_DIR}/unauth.status"
  : >"${RUN_DIR}/unauth.body"
  printf '\n=== relayd start mode=%s tcp_session_model_enabled=%s tcp_session_model_workers=%s tcp_session_model_sharded_accept=%s tcp_session_model_accept_balanced=%s tcp_splice_enabled=%s force_tcp_copy_fallback=%s ===\n' \
    "$mode_label" "$tcp_session_model_enabled" "$tcp_session_model_workers" "$tcp_session_model_sharded_accept" "$tcp_session_model_accept_balanced" "$tcp_splice_enabled" "$force_tcp_copy_fallback" >>"$RELAYD_LOG"

  log "starting relayd mode=${mode_label} on ${HTTP_LISTEN} with port range ${PORT_RANGE}"
  AUTH_TOKEN="$AUTH_TOKEN" \
  HTTP_LISTEN="$HTTP_LISTEN" \
  PORT_RANGE="$PORT_RANGE" \
  SQLITE_PATH="$sqlite_path" \
  TCP_SESSION_MODEL_ENABLED="$tcp_session_model_enabled" \
  TCP_SESSION_MODEL_WORKERS="$tcp_session_model_workers" \
  TCP_SESSION_MODEL_SHARDED_ACCEPT="$tcp_session_model_sharded_accept" \
  TCP_SESSION_MODEL_ACCEPT_BALANCED="$tcp_session_model_accept_balanced" \
  TCP_SPLICE_ENABLED="$tcp_splice_enabled" \
  FORCE_TCP_COPY_FALLBACK="$force_tcp_copy_fallback" \
  "$RELAYD_BIN" >>"$RELAYD_LOG" 2>&1 &
  RELAYD_PID=$!
  track_child "$RELAYD_PID"

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
}

stop_relayd() {
  if [[ -n "${RELAYD_PID:-}" ]] && kill -0 "$RELAYD_PID" 2>/dev/null; then
    kill "$RELAYD_PID" 2>/dev/null || true
    wait "$RELAYD_PID" 2>/dev/null || true
  fi
  RELAYD_PID=
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

wait_for_absent() {
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

    if ! allocation_field_from_list "$LATEST_LIST_JSON" "$allocation_id" runtime_status >/dev/null 2>&1; then
      return 0
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
  if [[ "$status" == "204" ]]; then
    wait_for_absent "$allocation_id" || die "allocation ${allocation_id} still present after delete"
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

  local attempt update_result
  for ((attempt = 1; attempt <= 20; attempt++)); do
    api_request "$update_status" "$update_body" POST /v1/ports/target "$AUTH_TOKEN" "{\"id\":\"${allocation_id}\",\"host\":\"${TARGET_HOST}\"}"
    update_result=$(<"$update_status")
    if [[ "$update_result" == "200" ]]; then
      break
    fi
    [[ "$update_result" == "404" ]] || break
    sleep 0.1
  done
  [[ "$update_result" == "200" ]] || die "${protocol} allocation host update failed with status ${update_result}"

  if ! wait_for_active "$allocation_id"; then
    die "${protocol} allocation ${allocation_id} did not become active"
  fi

  CREATED_ALLOCATION_ID=$allocation_id
  CREATED_RELAY_PORT=$relay_port
}

capture_metrics_snapshot() {
  local output_json=$1
  local status_file="${output_json}.status"
  api_request "$status_file" "$output_json" GET /v1/metrics "$AUTH_TOKEN"
  [[ $(<"$status_file") == "200" ]] || die "metrics request failed with status $(<"$status_file")"
}

append_udp_result() {
  local results_file=$1
  local path_label=$2
  local rate=$3
  local packet_size=$4
  local repetition=$5
  local duration=$6
  local json_file=$7
  local client_log=$8
  local server_log=$9
  python3 - "$results_file" "$path_label" "$rate" "$packet_size" "$repetition" "$duration" "$json_file" "$client_log" "$server_log" <<'PY'
import json
import pathlib
import sys

results_file = pathlib.Path(sys.argv[1])
path_label = sys.argv[2]
rate = sys.argv[3]
packet_size = int(sys.argv[4])
repetition = int(sys.argv[5])
duration = float(sys.argv[6])
json_file = pathlib.Path(sys.argv[7])
client_log = pathlib.Path(sys.argv[8])
server_log = pathlib.Path(sys.argv[9])
payload = json.load(open(json_file, encoding='utf-8'))
end = payload.get('end') or {}
summary = end.get('sum_received') or end.get('sum') or {}
record = {
    'path': path_label,
    'rate': rate,
    'packet_size': packet_size,
    'repetition': repetition,
    'duration_sec': duration,
    'bytes': summary.get('bytes', 0),
    'bits_per_second': summary.get('bits_per_second', 0.0),
    'jitter_ms': summary.get('jitter_ms', 0.0),
    'lost_packets': summary.get('lost_packets', 0),
    'packets': summary.get('packets', 0),
    'lost_percent': summary.get('lost_percent', 0.0),
    'json_file': json_file.relative_to(results_file.parent).as_posix(),
    'client_log': client_log.relative_to(results_file.parent).as_posix(),
    'server_log': server_log.relative_to(results_file.parent).as_posix(),
}
with open(results_file, 'a', encoding='utf-8') as handle:
    handle.write(json.dumps(record, sort_keys=True))
    handle.write('\n')
PY
}

append_tcp_result() {
  local results_file=$1
  local path_label=$2
  local repetition=$3
  local duration=$4
  local streams=$5
  local json_file=$6
  local client_log=$7
  local server_log=$8
  local before_metrics=${9}
  local after_metrics=${10}
  python3 - "$results_file" "$path_label" "$repetition" "$duration" "$streams" "$json_file" "$client_log" "$server_log" "$before_metrics" "$after_metrics" <<'PY'
import json
import pathlib
import sys

results_file = pathlib.Path(sys.argv[1])
path_label = sys.argv[2]
repetition = int(sys.argv[3])
duration = float(sys.argv[4])
streams = int(sys.argv[5])
json_file = pathlib.Path(sys.argv[6])
client_log = pathlib.Path(sys.argv[7])
server_log = pathlib.Path(sys.argv[8])
before_metrics_file = pathlib.Path(sys.argv[9])
after_metrics_file = pathlib.Path(sys.argv[10])
payload = json.load(open(json_file, encoding='utf-8'))
before_metrics = json.load(open(before_metrics_file, encoding='utf-8'))
after_metrics = json.load(open(after_metrics_file, encoding='utf-8'))
end = payload.get('end') or {}
summary = end.get('sum_received') or end.get('sum') or {}

def delta(key):
    return int(after_metrics.get(key, 0)) - int(before_metrics.get(key, 0))

record = {
    'path': path_label,
    'repetition': repetition,
    'duration_sec': duration,
    'streams': streams,
    'bytes': summary.get('bytes', 0),
    'bits_per_second': summary.get('bits_per_second', 0.0),
    'json_file': json_file.relative_to(results_file.parent).as_posix(),
    'client_log': client_log.relative_to(results_file.parent).as_posix(),
    'server_log': server_log.relative_to(results_file.parent).as_posix(),
    'metrics_before': before_metrics_file.relative_to(results_file.parent).as_posix(),
    'metrics_after': after_metrics_file.relative_to(results_file.parent).as_posix(),
    'tcp_splice_attempt_delta': delta('tcp_splice_attempt_total'),
    'tcp_splice_success_delta': delta('tcp_splice_success_total'),
    'tcp_splice_fallback_delta': delta('tcp_splice_fallback_total'),
    'tcp_splice_hard_failure_delta': delta('tcp_splice_hard_failure_total'),
    'tcp_splice_fallback_forced_delta': delta('tcp_splice_fallback_forced_total'),
    'tcp_splice_fallback_unsupported_delta': delta('tcp_splice_fallback_unsupported_total'),
    'tcp_splice_fallback_runtime_error_delta': delta('tcp_splice_fallback_runtime_error_total'),
    'tcp_session_create_delta': delta('tcp_session_create_total'),
    'tcp_session_close_delta': delta('tcp_session_close_total'),
    'tcp_session_event_delta': delta('tcp_session_event_total'),
    'tcp_session_worker_dispatch_delta': delta('tcp_session_worker_dispatch_total'),
    'tcp_session_worker0_dispatch_delta': delta('tcp_session_worker0_dispatch_total'),
    'tcp_session_worker1_dispatch_delta': delta('tcp_session_worker1_dispatch_total'),
    'tcp_accept_handoff_delta': delta('tcp_accept_handoff_total'),
    'tcp_accept_handoff_worker0_delta': delta('tcp_accept_handoff_worker0_total'),
    'tcp_accept_handoff_worker1_delta': delta('tcp_accept_handoff_worker1_total'),
    'tcp_accept_handoff_worker2_delta': delta('tcp_accept_handoff_worker2_total'),
    'tcp_accept_handoff_worker3_delta': delta('tcp_accept_handoff_worker3_total'),
    'tcp_listener_accept_delta': delta('tcp_listener_accept_total'),
    'tcp_listener_accept_worker0_delta': delta('tcp_listener_accept_worker0_total'),
    'tcp_listener_accept_worker1_delta': delta('tcp_listener_accept_worker1_total'),
    'tcp_listener_accept_worker2_delta': delta('tcp_listener_accept_worker2_total'),
    'tcp_listener_accept_worker3_delta': delta('tcp_listener_accept_worker3_total'),
    'tcp_upstream_connect_delta': delta('tcp_upstream_connect_total'),
    'tcp_upstream_connect_fail_delta': delta('tcp_upstream_connect_fail_total'),
    'tcp_active_sessions_after': int(after_metrics.get('tcp_active_sessions', 0)),
}
with open(results_file, 'a', encoding='utf-8') as handle:
    handle.write(json.dumps(record, sort_keys=True))
    handle.write('\n')
PY
}

emit_tcp_copy_vs_splice_summary() {
  local results_file=$1
  local summary_txt=$2
  local summary_json=$3
  local direct_json=$4
  local direct_log=$5
  local direct_server_log=$6
  local copy_json=$7
  local copy_log=$8
  local copy_server_log=$9
  local splice_json=${10}
  local splice_log=${11}
  local splice_server_log=${12}
  local legacy_tcp_json=${13}
  local legacy_tcp_log=${14}
  local legacy_tcp_server_log=${15}
  python3 - "$results_file" "$summary_txt" "$summary_json" "$direct_json" "$direct_log" "$direct_server_log" "$copy_json" "$copy_log" "$copy_server_log" "$splice_json" "$splice_log" "$splice_server_log" "$legacy_tcp_json" "$legacy_tcp_log" "$legacy_tcp_server_log" <<'PY'
import json
import pathlib
import shutil
import statistics
import sys

results_path = pathlib.Path(sys.argv[1])
summary_path = pathlib.Path(sys.argv[2])
summary_json_path = pathlib.Path(sys.argv[3])
direct_json_out = pathlib.Path(sys.argv[4])
direct_log_out = pathlib.Path(sys.argv[5])
direct_server_log_out = pathlib.Path(sys.argv[6])
copy_json_out = pathlib.Path(sys.argv[7])
copy_log_out = pathlib.Path(sys.argv[8])
copy_server_log_out = pathlib.Path(sys.argv[9])
splice_json_out = pathlib.Path(sys.argv[10])
splice_log_out = pathlib.Path(sys.argv[11])
splice_server_log_out = pathlib.Path(sys.argv[12])
legacy_tcp_json_out = pathlib.Path(sys.argv[13])
legacy_tcp_log_out = pathlib.Path(sys.argv[14])
legacy_tcp_server_log_out = pathlib.Path(sys.argv[15])
records = [json.loads(line) for line in results_path.read_text(encoding='utf-8').splitlines() if line.strip()]
if not records:
    raise SystemExit("no TCP copy-vs-splice records found")

UNITS_BPS = ['bps', 'kbps', 'mbps', 'gbps']

def format_decimal(value, units):
    value = float(value)
    unit_index = 0
    while value >= 1000.0 and unit_index < len(units) - 1:
        value /= 1000.0
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"

def summarize(items):
    throughputs = [item['bits_per_second'] for item in items]
    median_bps = statistics.median(throughputs)
    best_bps = max(throughputs)
    representative = min(
        items,
        key=lambda item: (abs(item['bits_per_second'] - median_bps), item['repetition']),
    )
    return {
        'samples': len(items),
        'median_bps': median_bps,
        'best_bps': best_bps,
        'duration_sec': items[0]['duration_sec'],
        'streams': items[0]['streams'],
        'representative': representative,
        'attempts': sum(item.get('tcp_splice_attempt_delta', 0) for item in items),
        'successes': sum(item.get('tcp_splice_success_delta', 0) for item in items),
        'fallbacks': sum(item.get('tcp_splice_fallback_delta', 0) for item in items),
        'hard_failures': sum(item.get('tcp_splice_hard_failure_delta', 0) for item in items),
    }

def copy_record_artifacts(record, json_out, log_out, server_log_out):
    base = results_path.parent
    shutil.copy2(base / record['json_file'], json_out)
    shutil.copy2(base / record['client_log'], log_out)
    shutil.copy2(base / record['server_log'], server_log_out)

grouped = {}
for record in records:
    grouped.setdefault(record['path'], []).append(record)

direct_records = sorted(grouped.get('direct', []), key=lambda item: item['repetition'])
copy_records = sorted(grouped.get('copy', []), key=lambda item: item['repetition'])
splice_records = sorted(grouped.get('splice', []), key=lambda item: item['repetition'])
if not direct_records or not copy_records or not splice_records:
    raise SystemExit("expected direct, copy, and splice TCP records")

direct = summarize(direct_records)
copy = summarize(copy_records)
splice = summarize(splice_records)
copy_record_artifacts(direct['representative'], direct_json_out, direct_log_out, direct_server_log_out)
copy_record_artifacts(copy['representative'], copy_json_out, copy_log_out, copy_server_log_out)
copy_record_artifacts(splice['representative'], splice_json_out, splice_log_out, splice_server_log_out)
shutil.copy2(splice_json_out, legacy_tcp_json_out)
shutil.copy2(splice_log_out, legacy_tcp_log_out)
shutil.copy2(splice_server_log_out, legacy_tcp_server_log_out)

copy_vs_splice_ratio = splice['median_bps'] / copy['median_bps'] if copy['median_bps'] else None
direct_vs_splice_ratio = direct['median_bps'] / splice['median_bps'] if splice['median_bps'] else None
threshold = 1.15 if direct['streams'] == 1 else 0.95
success_rate = splice['successes'] / splice['attempts'] if splice['attempts'] else 0.0
decision = 'splice not justified enough in current tcp architecture'
if (
    copy_vs_splice_ratio is not None
    and copy_vs_splice_ratio >= threshold
    and splice['hard_failures'] == 0
    and success_rate >= 0.90
):
    decision = 'splice remains justified in current tcp architecture'

summary_payload = {
    'streams': direct['streams'],
    'repetitions': direct['samples'],
    'duration_sec': direct['duration_sec'],
    'threshold': threshold,
    'decision': decision,
    'direct_median_bps': direct['median_bps'],
    'direct_best_bps': direct['best_bps'],
    'copy_median_bps': copy['median_bps'],
    'copy_best_bps': copy['best_bps'],
    'splice_median_bps': splice['median_bps'],
    'splice_best_bps': splice['best_bps'],
    'copy_vs_splice_ratio': copy_vs_splice_ratio,
    'direct_vs_splice_ratio': direct_vs_splice_ratio,
    'splice_attempts': splice['attempts'],
    'splice_successes': splice['successes'],
    'splice_fallbacks': splice['fallbacks'],
    'splice_hard_failures': splice['hard_failures'],
    'splice_success_rate': success_rate,
}
summary_json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')

copy_vs_splice_text = 'n/a' if copy_vs_splice_ratio is None else f"{copy_vs_splice_ratio:.2f}x"
direct_vs_splice_text = 'n/a' if direct_vs_splice_ratio is None else f"{direct_vs_splice_ratio:.2f}x"
lines = [
    '=== relayd tcp copy vs splice summary ===',
    f"repetitions: {direct['samples']}",
    f"duration:    {direct['duration_sec']:g}s",
    f"streams:     {direct['streams']}",
    f"decision threshold: splice/copy >= {threshold:.2f}x",
    '',
    f"direct throughput: {format_decimal(direct['median_bps'], UNITS_BPS)} median / {format_decimal(direct['best_bps'], UNITS_BPS)} best",
    f"relay-copy throughput:   {format_decimal(copy['median_bps'], UNITS_BPS)} median / {format_decimal(copy['best_bps'], UNITS_BPS)} best",
    f"relay-splice throughput: {format_decimal(splice['median_bps'], UNITS_BPS)} median / {format_decimal(splice['best_bps'], UNITS_BPS)} best",
    f"splice/copy ratio: {copy_vs_splice_text}",
    f"direct/splice ratio: {direct_vs_splice_text}",
    f"splice hard failures: {splice['hard_failures']}",
    f"splice success-rate note: {splice['successes']}/{splice['attempts']} sessions ({success_rate * 100.0:.2f}%)",
    f"direct artifact: {direct['representative']['json_file']} | {direct['representative']['client_log']}",
    f"copy artifact:   {copy['representative']['json_file']} | {copy['representative']['client_log']}",
    f"splice artifact: {splice['representative']['json_file']} | {splice['representative']['client_log']}",
    decision,
]
summary_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print(summary_path.read_text(encoding='utf-8'), end='')
PY
}

refresh_tcp_copy_vs_splice_overall_summary() {
  local latest_dir=$1
  python3 - "$latest_dir" <<'PY'
import json
import pathlib
import sys

latest_dir = pathlib.Path(sys.argv[1])
summary_paths = sorted(latest_dir.glob("tcp-copy-vs-splice-streams-*-summary.json"))
overall_path = latest_dir / "tcp-copy-vs-splice-overall-summary.txt"
if not summary_paths:
    overall_path.write_text("=== relayd tcp copy vs splice overall summary ===\nno stream summaries available\n", encoding="utf-8")
    print(overall_path.read_text(encoding="utf-8"), end="")
    raise SystemExit(0)

UNITS_BPS = ['bps', 'kbps', 'mbps', 'gbps']

def format_decimal(value, units):
    value = float(value)
    unit_index = 0
    while value >= 1000.0 and unit_index < len(units) - 1:
        value /= 1000.0
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"

summaries = [json.loads(path.read_text(encoding="utf-8")) for path in summary_paths]
lines = [
    "=== relayd tcp copy vs splice overall summary ===",
]
for item in summaries:
    ratio = item.get("copy_vs_splice_ratio")
    ratio_text = "n/a" if ratio is None else f"{ratio:.2f}x"
    lines.append(
        f"streams={item['streams']}: copy {format_decimal(item['copy_median_bps'], UNITS_BPS)}, "
        f"splice {format_decimal(item['splice_median_bps'], UNITS_BPS)}, "
        f"splice/copy {ratio_text}, hard_failures={item['splice_hard_failures']}, "
        f"success_rate={item['splice_success_rate'] * 100.0:.2f}% -> {item['decision']}"
    )
overall_decision = (
    "splice remains justified in current tcp architecture"
    if all(item['decision'] == "splice remains justified in current tcp architecture" for item in summaries)
    else "splice not justified enough in current tcp architecture"
)
lines.extend(["", overall_decision])
overall_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(overall_path.read_text(encoding="utf-8"), end="")
PY
}

emit_tcp_session_model_summary() {
  local results_file=$1
  local summary_txt=$2
  local summary_json=$3
  local direct_json=$4
  local direct_log=$5
  local direct_server_log=$6
  local threaded_json=$7
  local threaded_log=$8
  local threaded_server_log=$9
  local session_model_json=${10}
  local session_model_log=${11}
  local session_model_server_log=${12}
  local legacy_tcp_json=${13}
  local legacy_tcp_log=${14}
  local legacy_tcp_server_log=${15}
  python3 - "$results_file" "$summary_txt" "$summary_json" "$direct_json" "$direct_log" "$direct_server_log" "$threaded_json" "$threaded_log" "$threaded_server_log" "$session_model_json" "$session_model_log" "$session_model_server_log" "$legacy_tcp_json" "$legacy_tcp_log" "$legacy_tcp_server_log" <<'PY'
import json
import pathlib
import shutil
import statistics
import sys

results_path = pathlib.Path(sys.argv[1])
summary_path = pathlib.Path(sys.argv[2])
summary_json_path = pathlib.Path(sys.argv[3])
direct_json_out = pathlib.Path(sys.argv[4])
direct_log_out = pathlib.Path(sys.argv[5])
direct_server_log_out = pathlib.Path(sys.argv[6])
threaded_json_out = pathlib.Path(sys.argv[7])
threaded_log_out = pathlib.Path(sys.argv[8])
threaded_server_log_out = pathlib.Path(sys.argv[9])
session_json_out = pathlib.Path(sys.argv[10])
session_log_out = pathlib.Path(sys.argv[11])
session_server_log_out = pathlib.Path(sys.argv[12])
legacy_tcp_json_out = pathlib.Path(sys.argv[13])
legacy_tcp_log_out = pathlib.Path(sys.argv[14])
legacy_tcp_server_log_out = pathlib.Path(sys.argv[15])
records = [json.loads(line) for line in results_path.read_text(encoding='utf-8').splitlines() if line.strip()]
if not records:
    raise SystemExit("no TCP session-model records found")

UNITS_BPS = ['bps', 'kbps', 'mbps', 'gbps']

def format_decimal(value, units):
    value = float(value)
    unit_index = 0
    while value >= 1000.0 and unit_index < len(units) - 1:
        value /= 1000.0
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"

def summarize(items):
    throughputs = [item['bits_per_second'] for item in items]
    median_bps = statistics.median(throughputs)
    best_bps = max(throughputs)
    representative = min(
        items,
        key=lambda item: (abs(item['bits_per_second'] - median_bps), item['repetition']),
    )
    return {
        'samples': len(items),
        'median_bps': median_bps,
        'best_bps': best_bps,
        'duration_sec': items[0]['duration_sec'],
        'streams': items[0]['streams'],
        'representative': representative,
        'session_creates': sum(item.get('tcp_session_create_delta', 0) for item in items),
        'session_closes': sum(item.get('tcp_session_close_delta', 0) for item in items),
        'session_events': sum(item.get('tcp_session_event_delta', 0) for item in items),
        'max_active_after': max(item.get('tcp_active_sessions_after', 0) for item in items),
    }

def copy_record_artifacts(record, json_out, log_out, server_log_out):
    base = results_path.parent
    shutil.copy2(base / record['json_file'], json_out)
    shutil.copy2(base / record['client_log'], log_out)
    shutil.copy2(base / record['server_log'], server_log_out)

grouped = {}
for record in records:
    grouped.setdefault(record['path'], []).append(record)

direct_records = sorted(grouped.get('direct', []), key=lambda item: item['repetition'])
threaded_records = sorted(grouped.get('threaded', []), key=lambda item: item['repetition'])
session_records = sorted(grouped.get('session-model', []), key=lambda item: item['repetition'])
if not direct_records or not threaded_records or not session_records:
    raise SystemExit("expected direct, threaded, and session-model TCP records")

direct = summarize(direct_records)
threaded = summarize(threaded_records)
session = summarize(session_records)
copy_record_artifacts(direct['representative'], direct_json_out, direct_log_out, direct_server_log_out)
copy_record_artifacts(threaded['representative'], threaded_json_out, threaded_log_out, threaded_server_log_out)
copy_record_artifacts(session['representative'], session_json_out, session_log_out, session_server_log_out)
shutil.copy2(session_json_out, legacy_tcp_json_out)
shutil.copy2(session_log_out, legacy_tcp_log_out)
shutil.copy2(session_server_log_out, legacy_tcp_server_log_out)

new_vs_old_ratio = session['median_bps'] / threaded['median_bps'] if threaded['median_bps'] else None
direct_vs_new_ratio = direct['median_bps'] / session['median_bps'] if session['median_bps'] else None
threshold = 1.15 if direct['streams'] == 1 else 1.05
decision = 'tcp session-model change did not clear the next gate in current runtime architecture'
if (
    new_vs_old_ratio is not None
    and new_vs_old_ratio >= threshold
    and session['max_active_after'] == 0
    and session['session_creates'] > 0
    and session['session_closes'] >= session['session_creates']
):
    decision = 'tcp session-model change is justified in current runtime architecture'

summary_payload = {
    'streams': direct['streams'],
    'repetitions': direct['samples'],
    'duration_sec': direct['duration_sec'],
    'threshold': threshold,
    'decision': decision,
    'direct_median_bps': direct['median_bps'],
    'direct_best_bps': direct['best_bps'],
    'threaded_median_bps': threaded['median_bps'],
    'threaded_best_bps': threaded['best_bps'],
    'session_model_median_bps': session['median_bps'],
    'session_model_best_bps': session['best_bps'],
    'new_vs_old_ratio': new_vs_old_ratio,
    'direct_vs_new_ratio': direct_vs_new_ratio,
    'session_creates': session['session_creates'],
    'session_closes': session['session_closes'],
    'session_events': session['session_events'],
    'max_active_after': session['max_active_after'],
}
summary_json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')

ratio_text = 'n/a' if new_vs_old_ratio is None else f"{new_vs_old_ratio:.2f}x"
direct_ratio_text = 'n/a' if direct_vs_new_ratio is None else f"{direct_vs_new_ratio:.2f}x"
lines = [
    '=== relayd tcp session-model summary ===',
    f"repetitions: {direct['samples']}",
    f"duration:    {direct['duration_sec']:g}s",
    f"streams:     {direct['streams']}",
    f"decision threshold: new/old >= {threshold:.2f}x",
    '',
    f"direct throughput: {format_decimal(direct['median_bps'], UNITS_BPS)} median / {format_decimal(direct['best_bps'], UNITS_BPS)} best",
    f"threaded throughput:      {format_decimal(threaded['median_bps'], UNITS_BPS)} median / {format_decimal(threaded['best_bps'], UNITS_BPS)} best",
    f"session-model throughput: {format_decimal(session['median_bps'], UNITS_BPS)} median / {format_decimal(session['best_bps'], UNITS_BPS)} best",
    f"new/old ratio: {ratio_text}",
    f"direct/new ratio: {direct_ratio_text}",
    f"session-model creates/closes/events: {session['session_creates']}/{session['session_closes']}/{session['session_events']}",
    f"session-model active-after note: max active sessions after run = {session['max_active_after']}",
    f"direct artifact: {direct['representative']['json_file']} | {direct['representative']['client_log']}",
    f"threaded artifact:      {threaded['representative']['json_file']} | {threaded['representative']['client_log']}",
    f"session-model artifact: {session['representative']['json_file']} | {session['representative']['client_log']}",
    decision,
]
summary_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print(summary_path.read_text(encoding='utf-8'), end='')
PY
}

refresh_tcp_session_model_overall_summary() {
  local latest_dir=$1
  python3 - "$latest_dir" <<'PY'
import json
import pathlib
import sys

latest_dir = pathlib.Path(sys.argv[1])
summary_paths = sorted(latest_dir.glob("tcp-session-model-streams-*-summary.json"))
overall_path = latest_dir / "tcp-session-model-overall-summary.txt"
if not summary_paths:
    overall_path.write_text("=== relayd tcp session-model overall summary ===\nno stream summaries available\n", encoding="utf-8")
    print(overall_path.read_text(encoding="utf-8"), end="")
    raise SystemExit(0)

UNITS_BPS = ['bps', 'kbps', 'mbps', 'gbps']

def format_decimal(value, units):
    value = float(value)
    unit_index = 0
    while value >= 1000.0 and unit_index < len(units) - 1:
        value /= 1000.0
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"

summaries = [json.loads(path.read_text(encoding="utf-8")) for path in summary_paths]
lines = [
    "=== relayd tcp session-model overall summary ===",
]
for item in summaries:
    ratio = item.get("new_vs_old_ratio")
    ratio_text = "n/a" if ratio is None else f"{ratio:.2f}x"
    lines.append(
        f"streams={item['streams']}: threaded {format_decimal(item['threaded_median_bps'], UNITS_BPS)}, "
        f"session-model {format_decimal(item['session_model_median_bps'], UNITS_BPS)}, "
        f"new/old {ratio_text}, active_after={item['max_active_after']}, "
        f"creates/closes/events={item['session_creates']}/{item['session_closes']}/{item['session_events']} -> {item['decision']}"
    )
overall_decision = (
    "tcp session-model change is justified in current runtime architecture"
    if all(item['decision'] == "tcp session-model change is justified in current runtime architecture" for item in summaries)
    else "tcp session-model change did not clear the next gate in current runtime architecture"
)
lines.extend(["", overall_decision])
overall_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(overall_path.read_text(encoding="utf-8"), end="")
PY
}

emit_tcp_workerized_session_summary() {
  local results_file=$1
  local summary_txt=$2
  local summary_json=$3
  local direct_json=$4
  local direct_log=$5
  local direct_server_log=$6
  local threaded_json=$7
  local threaded_log=$8
  local threaded_server_log=$9
  local session_json=${10}
  local session_log=${11}
  local session_server_log=${12}
  local workerized_json=${13}
  local workerized_log=${14}
  local workerized_server_log=${15}
  local legacy_tcp_json=${16}
  local legacy_tcp_log=${17}
  local legacy_tcp_server_log=${18}
  python3 - "$results_file" "$summary_txt" "$summary_json" "$direct_json" "$direct_log" "$direct_server_log" "$threaded_json" "$threaded_log" "$threaded_server_log" "$session_json" "$session_log" "$session_server_log" "$workerized_json" "$workerized_log" "$workerized_server_log" "$legacy_tcp_json" "$legacy_tcp_log" "$legacy_tcp_server_log" <<'PY'
import json
import pathlib
import shutil
import statistics
import sys

results_path = pathlib.Path(sys.argv[1])
summary_path = pathlib.Path(sys.argv[2])
summary_json_path = pathlib.Path(sys.argv[3])
direct_json_out = pathlib.Path(sys.argv[4]); direct_log_out = pathlib.Path(sys.argv[5]); direct_server_log_out = pathlib.Path(sys.argv[6])
threaded_json_out = pathlib.Path(sys.argv[7]); threaded_log_out = pathlib.Path(sys.argv[8]); threaded_server_log_out = pathlib.Path(sys.argv[9])
session_json_out = pathlib.Path(sys.argv[10]); session_log_out = pathlib.Path(sys.argv[11]); session_server_log_out = pathlib.Path(sys.argv[12])
workerized_json_out = pathlib.Path(sys.argv[13]); workerized_log_out = pathlib.Path(sys.argv[14]); workerized_server_log_out = pathlib.Path(sys.argv[15])
legacy_tcp_json_out = pathlib.Path(sys.argv[16]); legacy_tcp_log_out = pathlib.Path(sys.argv[17]); legacy_tcp_server_log_out = pathlib.Path(sys.argv[18])
records = [json.loads(line) for line in results_path.read_text(encoding='utf-8').splitlines() if line.strip()]
if not records:
    raise SystemExit("no TCP workerized-session records found")

UNITS_BPS = ['bps', 'kbps', 'mbps', 'gbps']
def format_decimal(value, units):
    value = float(value)
    unit_index = 0
    while value >= 1000.0 and unit_index < len(units) - 1:
        value /= 1000.0
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"

def summarize(items):
    throughputs = [item['bits_per_second'] for item in items]
    median_bps = statistics.median(throughputs)
    best_bps = max(throughputs)
    representative = min(items, key=lambda item: (abs(item['bits_per_second'] - median_bps), item['repetition']))
    return {
        'samples': len(items),
        'median_bps': median_bps,
        'best_bps': best_bps,
        'duration_sec': items[0]['duration_sec'],
        'streams': items[0]['streams'],
        'representative': representative,
        'session_creates': sum(item.get('tcp_session_create_delta', 0) for item in items),
        'session_closes': sum(item.get('tcp_session_close_delta', 0) for item in items),
        'session_events': sum(item.get('tcp_session_event_delta', 0) for item in items),
        'worker_dispatches': sum(item.get('tcp_session_worker_dispatch_delta', 0) for item in items),
        'worker0_dispatches': sum(item.get('tcp_session_worker0_dispatch_delta', 0) for item in items),
        'worker1_dispatches': sum(item.get('tcp_session_worker1_dispatch_delta', 0) for item in items),
        'max_active_after': max(item.get('tcp_active_sessions_after', 0) for item in items),
    }

def copy_record_artifacts(record, json_out, log_out, server_log_out):
    base = results_path.parent
    shutil.copy2(base / record['json_file'], json_out)
    shutil.copy2(base / record['client_log'], log_out)
    shutil.copy2(base / record['server_log'], server_log_out)

grouped = {}
for record in records:
    grouped.setdefault(record['path'], []).append(record)
direct = summarize(sorted(grouped.get('direct', []), key=lambda item: item['repetition']))
threaded = summarize(sorted(grouped.get('threaded', []), key=lambda item: item['repetition']))
session = summarize(sorted(grouped.get('session-model', []), key=lambda item: item['repetition']))
workerized = summarize(sorted(grouped.get('workerized-session', []), key=lambda item: item['repetition']))
if any(item['samples'] == 0 for item in (direct, threaded, session, workerized)):
    raise SystemExit("expected direct, threaded, session-model, and workerized-session records")

copy_record_artifacts(direct['representative'], direct_json_out, direct_log_out, direct_server_log_out)
copy_record_artifacts(threaded['representative'], threaded_json_out, threaded_log_out, threaded_server_log_out)
copy_record_artifacts(session['representative'], session_json_out, session_log_out, session_server_log_out)
copy_record_artifacts(workerized['representative'], workerized_json_out, workerized_log_out, workerized_server_log_out)
shutil.copy2(workerized_json_out, legacy_tcp_json_out)
shutil.copy2(workerized_log_out, legacy_tcp_log_out)
shutil.copy2(workerized_server_log_out, legacy_tcp_server_log_out)

workerized_vs_session = workerized['median_bps'] / session['median_bps'] if session['median_bps'] else None
workerized_vs_threaded = workerized['median_bps'] / threaded['median_bps'] if threaded['median_bps'] else None
direct_vs_workerized = direct['median_bps'] / workerized['median_bps'] if workerized['median_bps'] else None
streams = direct['streams']
decision = 'workerized tcp session-model did not clear the next gate in current runtime architecture'
if (
    workerized['max_active_after'] == 0
    and workerized['session_creates'] > 0
    and workerized['session_closes'] >= workerized['session_creates']
    and workerized['worker0_dispatches'] > 0
    and workerized['worker1_dispatches'] > 0
    and workerized_vs_session is not None
    and workerized_vs_threaded is not None
    and ((streams == 1 and workerized_vs_session >= 0.95) or (streams != 1 and workerized_vs_session >= 1.25 and workerized_vs_threaded >= 1.05))
):
    decision = 'workerized tcp session-model is justified in current runtime architecture'

summary_payload = {
    'streams': streams,
    'repetitions': direct['samples'],
    'duration_sec': direct['duration_sec'],
    'decision': decision,
    'direct_median_bps': direct['median_bps'],
    'threaded_median_bps': threaded['median_bps'],
    'session_model_median_bps': session['median_bps'],
    'workerized_median_bps': workerized['median_bps'],
    'workerized_vs_session_ratio': workerized_vs_session,
    'workerized_vs_threaded_ratio': workerized_vs_threaded,
    'direct_vs_workerized_ratio': direct_vs_workerized,
    'workerized_session_creates': workerized['session_creates'],
    'workerized_session_closes': workerized['session_closes'],
    'workerized_session_events': workerized['session_events'],
    'workerized_worker_dispatches': workerized['worker_dispatches'],
    'workerized_worker0_dispatches': workerized['worker0_dispatches'],
    'workerized_worker1_dispatches': workerized['worker1_dispatches'],
    'workerized_max_active_after': workerized['max_active_after'],
}
summary_json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')

def fmt_ratio(value):
    return 'n/a' if value is None else f"{value:.2f}x"

lines = [
    '=== relayd tcp workerized session summary ===',
    f"repetitions: {direct['samples']}",
    f"duration:    {direct['duration_sec']:g}s",
    f"streams:     {streams}",
    '',
    f"direct throughput:                {format_decimal(direct['median_bps'], UNITS_BPS)} median / {format_decimal(direct['best_bps'], UNITS_BPS)} best",
    f"threaded throughput:              {format_decimal(threaded['median_bps'], UNITS_BPS)} median / {format_decimal(threaded['best_bps'], UNITS_BPS)} best",
    f"single-thread session throughput: {format_decimal(session['median_bps'], UNITS_BPS)} median / {format_decimal(session['best_bps'], UNITS_BPS)} best",
    f"workerized throughput:            {format_decimal(workerized['median_bps'], UNITS_BPS)} median / {format_decimal(workerized['best_bps'], UNITS_BPS)} best",
    f"workerized/session ratio: {fmt_ratio(workerized_vs_session)}",
    f"workerized/threaded ratio: {fmt_ratio(workerized_vs_threaded)}",
    f"direct/workerized ratio: {fmt_ratio(direct_vs_workerized)}",
    f"workerized dispatch note: total={workerized['worker_dispatches']} worker0={workerized['worker0_dispatches']} worker1={workerized['worker1_dispatches']}",
    f"workerized active-after note: max active sessions after run = {workerized['max_active_after']}",
    f"direct artifact:     {direct['representative']['json_file']} | {direct['representative']['client_log']}",
    f"threaded artifact:   {threaded['representative']['json_file']} | {threaded['representative']['client_log']}",
    f"session artifact:    {session['representative']['json_file']} | {session['representative']['client_log']}",
    f"workerized artifact: {workerized['representative']['json_file']} | {workerized['representative']['client_log']}",
    decision,
]
summary_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print(summary_path.read_text(encoding='utf-8'), end='')
PY
}

refresh_tcp_workerized_session_overall_summary() {
  local latest_dir=$1
  python3 - "$latest_dir" <<'PY'
import json
import pathlib
import sys

latest_dir = pathlib.Path(sys.argv[1])
summary_paths = sorted(latest_dir.glob("tcp-workerized-session-streams-*-summary.json"))
overall_path = latest_dir / "tcp-workerized-session-overall-summary.txt"
if not summary_paths:
    overall_path.write_text("=== relayd tcp workerized session overall summary ===\nno stream summaries available\n", encoding="utf-8")
    print(overall_path.read_text(encoding="utf-8"), end="")
    raise SystemExit(0)

UNITS_BPS = ['bps', 'kbps', 'mbps', 'gbps']
def format_decimal(value, units):
    value = float(value)
    unit_index = 0
    while value >= 1000.0 and unit_index < len(units) - 1:
        value /= 1000.0
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"

summaries = [json.loads(path.read_text(encoding='utf-8')) for path in summary_paths]
lines = ["=== relayd tcp workerized session overall summary ==="]
for item in summaries:
    lines.append(
        f"streams={item['streams']}: threaded {format_decimal(item['threaded_median_bps'], UNITS_BPS)}, "
        f"single-thread {format_decimal(item['session_model_median_bps'], UNITS_BPS)}, "
        f"workerized {format_decimal(item['workerized_median_bps'], UNITS_BPS)}, "
        f"w/session={('n/a' if item['workerized_vs_session_ratio'] is None else f'{item['workerized_vs_session_ratio']:.2f}x')}, "
        f"w/threaded={('n/a' if item['workerized_vs_threaded_ratio'] is None else f'{item['workerized_vs_threaded_ratio']:.2f}x')} "
        f"dispatch={item['workerized_worker0_dispatches']}/{item['workerized_worker1_dispatches']} "
        f"active_after={item['workerized_max_active_after']} -> {item['decision']}"
    )
overall_decision = (
    'workerized tcp session-model is justified in current runtime architecture'
    if all(item['decision'] == 'workerized tcp session-model is justified in current runtime architecture' for item in summaries)
    else 'workerized tcp session-model did not clear the next gate in current runtime architecture'
)
lines.extend(['', overall_decision])
overall_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print(overall_path.read_text(encoding='utf-8'), end='')
PY
}

emit_tcp_sharded_worker_summary() {
  local results_file=$1
  local summary_txt=$2
  local summary_json=$3
  local direct_json=$4
  local direct_log=$5
  local direct_server_log=$6
  local threaded_json=$7
  local threaded_log=$8
  local threaded_server_log=$9
  local session_json=${10}
  local session_log=${11}
  local session_server_log=${12}
  local workerized_json=${13}
  local workerized_log=${14}
  local workerized_server_log=${15}
  local sharded_json=${16}
  local sharded_log=${17}
  local sharded_server_log=${18}
  local legacy_tcp_json=${19}
  local legacy_tcp_log=${20}
  local legacy_tcp_server_log=${21}
  python3 - "$results_file" "$summary_txt" "$summary_json" "$direct_json" "$direct_log" "$direct_server_log" "$threaded_json" "$threaded_log" "$threaded_server_log" "$session_json" "$session_log" "$session_server_log" "$workerized_json" "$workerized_log" "$workerized_server_log" "$sharded_json" "$sharded_log" "$sharded_server_log" "$legacy_tcp_json" "$legacy_tcp_log" "$legacy_tcp_server_log" <<'PY'
import json
import pathlib
import shutil
import statistics
import sys

results_path = pathlib.Path(sys.argv[1]); summary_path = pathlib.Path(sys.argv[2]); summary_json_path = pathlib.Path(sys.argv[3])
direct_json_out = pathlib.Path(sys.argv[4]); direct_log_out = pathlib.Path(sys.argv[5]); direct_server_log_out = pathlib.Path(sys.argv[6])
threaded_json_out = pathlib.Path(sys.argv[7]); threaded_log_out = pathlib.Path(sys.argv[8]); threaded_server_log_out = pathlib.Path(sys.argv[9])
session_json_out = pathlib.Path(sys.argv[10]); session_log_out = pathlib.Path(sys.argv[11]); session_server_log_out = pathlib.Path(sys.argv[12])
workerized_json_out = pathlib.Path(sys.argv[13]); workerized_log_out = pathlib.Path(sys.argv[14]); workerized_server_log_out = pathlib.Path(sys.argv[15])
sharded_json_out = pathlib.Path(sys.argv[16]); sharded_log_out = pathlib.Path(sys.argv[17]); sharded_server_log_out = pathlib.Path(sys.argv[18])
legacy_tcp_json_out = pathlib.Path(sys.argv[19]); legacy_tcp_log_out = pathlib.Path(sys.argv[20]); legacy_tcp_server_log_out = pathlib.Path(sys.argv[21])
records = [json.loads(line) for line in results_path.read_text(encoding='utf-8').splitlines() if line.strip()]
if not records:
    raise SystemExit("no TCP sharded-worker records found")

UNITS_BPS = ['bps', 'kbps', 'mbps', 'gbps']
def format_decimal(value, units):
    value = float(value); unit_index = 0
    while value >= 1000.0 and unit_index < len(units) - 1:
        value /= 1000.0; unit_index += 1
    return f"{value:.2f} {units[unit_index]}"

def summarize(items):
    throughputs = [item['bits_per_second'] for item in items]
    median_bps = statistics.median(throughputs)
    best_bps = max(throughputs)
    representative = min(items, key=lambda item: (abs(item['bits_per_second'] - median_bps), item['repetition']))
    return {
        'samples': len(items),
        'median_bps': median_bps,
        'best_bps': best_bps,
        'duration_sec': items[0]['duration_sec'],
        'streams': items[0]['streams'],
        'representative': representative,
        'session_creates': sum(item.get('tcp_session_create_delta', 0) for item in items),
        'session_closes': sum(item.get('tcp_session_close_delta', 0) for item in items),
        'listener_accepts': sum(item.get('tcp_listener_accept_delta', 0) for item in items),
        'accept_w0': sum(item.get('tcp_listener_accept_worker0_delta', 0) for item in items),
        'accept_w1': sum(item.get('tcp_listener_accept_worker1_delta', 0) for item in items),
        'accept_w2': sum(item.get('tcp_listener_accept_worker2_delta', 0) for item in items),
        'accept_w3': sum(item.get('tcp_listener_accept_worker3_delta', 0) for item in items),
        'connects': sum(item.get('tcp_upstream_connect_delta', 0) for item in items),
        'connect_fails': sum(item.get('tcp_upstream_connect_fail_delta', 0) for item in items),
        'max_active_after': max(item.get('tcp_active_sessions_after', 0) for item in items),
    }

def copy_record_artifacts(record, json_out, log_out, server_log_out):
    base = results_path.parent
    shutil.copy2(base / record['json_file'], json_out)
    shutil.copy2(base / record['client_log'], log_out)
    shutil.copy2(base / record['server_log'], server_log_out)

grouped = {}
for record in records:
    grouped.setdefault(record['path'], []).append(record)

direct = summarize(sorted(grouped.get('direct', []), key=lambda item: item['repetition']))
threaded = summarize(sorted(grouped.get('threaded', []), key=lambda item: item['repetition']))
session = summarize(sorted(grouped.get('session-model', []), key=lambda item: item['repetition']))
workerized = summarize(sorted(grouped.get('workerized-session', []), key=lambda item: item['repetition']))
sharded = summarize(sorted(grouped.get('sharded-worker', []), key=lambda item: item['repetition']))
if any(item['samples'] == 0 for item in (direct, threaded, session, workerized, sharded)):
    raise SystemExit("expected direct, threaded, session-model, workerized-session, and sharded-worker records")

copy_record_artifacts(direct['representative'], direct_json_out, direct_log_out, direct_server_log_out)
copy_record_artifacts(threaded['representative'], threaded_json_out, threaded_log_out, threaded_server_log_out)
copy_record_artifacts(session['representative'], session_json_out, session_log_out, session_server_log_out)
copy_record_artifacts(workerized['representative'], workerized_json_out, workerized_log_out, workerized_server_log_out)
copy_record_artifacts(sharded['representative'], sharded_json_out, sharded_log_out, sharded_server_log_out)
shutil.copy2(sharded_json_out, legacy_tcp_json_out)
shutil.copy2(sharded_log_out, legacy_tcp_log_out)
shutil.copy2(sharded_server_log_out, legacy_tcp_server_log_out)

sharded_vs_workerized = sharded['median_bps'] / workerized['median_bps'] if workerized['median_bps'] else None
sharded_vs_threaded = sharded['median_bps'] / threaded['median_bps'] if threaded['median_bps'] else None
sharded_vs_session = sharded['median_bps'] / session['median_bps'] if session['median_bps'] else None
direct_vs_sharded = direct['median_bps'] / sharded['median_bps'] if sharded['median_bps'] else None
streams = direct['streams']
decision = 'sharded-worker tcp model did not clear the next gate in current runtime architecture'
if (
    sharded['max_active_after'] == 0 and sharded['connect_fails'] == 0
    and sharded['accept_w0'] > 0 and sharded['accept_w1'] > 0 and sharded['accept_w2'] > 0 and sharded['accept_w3'] > 0
    and sharded_vs_workerized is not None and sharded_vs_threaded is not None and sharded_vs_session is not None
    and ((streams == 1 and sharded_vs_workerized >= 0.95) or (streams != 1 and sharded_vs_workerized >= 1.25 and sharded_vs_threaded >= 0.95))
):
    decision = 'sharded-worker tcp model is justified in current runtime architecture'

summary_payload = {
    'streams': streams,
    'repetitions': direct['samples'],
    'duration_sec': direct['duration_sec'],
    'decision': decision,
    'direct_median_bps': direct['median_bps'],
    'threaded_median_bps': threaded['median_bps'],
    'session_model_median_bps': session['median_bps'],
    'workerized_median_bps': workerized['median_bps'],
    'sharded_median_bps': sharded['median_bps'],
    'sharded_vs_workerized_ratio': sharded_vs_workerized,
    'sharded_vs_threaded_ratio': sharded_vs_threaded,
    'sharded_vs_session_ratio': sharded_vs_session,
    'direct_vs_sharded_ratio': direct_vs_sharded,
    'sharded_listener_accept_total': sharded['listener_accepts'],
    'sharded_accept_w0': sharded['accept_w0'],
    'sharded_accept_w1': sharded['accept_w1'],
    'sharded_accept_w2': sharded['accept_w2'],
    'sharded_accept_w3': sharded['accept_w3'],
    'sharded_connect_total': sharded['connects'],
    'sharded_connect_fail_total': sharded['connect_fails'],
    'sharded_max_active_after': sharded['max_active_after'],
}
summary_json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')

def fmt_ratio(value):
    return 'n/a' if value is None else f"{value:.2f}x"

lines = [
    '=== relayd tcp sharded worker summary ===',
    f"repetitions: {direct['samples']}",
    f"duration:    {direct['duration_sec']:g}s",
    f"streams:     {streams}",
    '',
    f"direct throughput:                 {format_decimal(direct['median_bps'], UNITS_BPS)} median / {format_decimal(direct['best_bps'], UNITS_BPS)} best",
    f"threaded throughput:               {format_decimal(threaded['median_bps'], UNITS_BPS)} median / {format_decimal(threaded['best_bps'], UNITS_BPS)} best",
    f"single-thread session throughput:  {format_decimal(session['median_bps'], UNITS_BPS)} median / {format_decimal(session['best_bps'], UNITS_BPS)} best",
    f"workerized throughput:             {format_decimal(workerized['median_bps'], UNITS_BPS)} median / {format_decimal(workerized['best_bps'], UNITS_BPS)} best",
    f"sharded-worker throughput:         {format_decimal(sharded['median_bps'], UNITS_BPS)} median / {format_decimal(sharded['best_bps'], UNITS_BPS)} best",
    f"sharded/workerized ratio: {fmt_ratio(sharded_vs_workerized)}",
    f"sharded/threaded ratio: {fmt_ratio(sharded_vs_threaded)}",
    f"sharded/session ratio: {fmt_ratio(sharded_vs_session)}",
    f"direct/sharded ratio: {fmt_ratio(direct_vs_sharded)}",
    f"sharded accept note: total={sharded['listener_accepts']} w0={sharded['accept_w0']} w1={sharded['accept_w1']} w2={sharded['accept_w2']} w3={sharded['accept_w3']}",
    f"sharded connect note: total={sharded['connects']} fail={sharded['connect_fails']}",
    f"sharded active-after note: max active sessions after run = {sharded['max_active_after']}",
    f"direct artifact:     {direct['representative']['json_file']} | {direct['representative']['client_log']}",
    f"threaded artifact:   {threaded['representative']['json_file']} | {threaded['representative']['client_log']}",
    f"session artifact:    {session['representative']['json_file']} | {session['representative']['client_log']}",
    f"workerized artifact: {workerized['representative']['json_file']} | {workerized['representative']['client_log']}",
    f"sharded artifact:    {sharded['representative']['json_file']} | {sharded['representative']['client_log']}",
    decision,
]
summary_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print(summary_path.read_text(encoding='utf-8'), end='')
PY
}

refresh_tcp_sharded_worker_overall_summary() {
  local latest_dir=$1
  python3 - "$latest_dir" <<'PY'
import json
import pathlib
import sys

latest_dir = pathlib.Path(sys.argv[1])
summary_paths = sorted(latest_dir.glob("tcp-sharded-worker-streams-*-summary.json"))
overall_path = latest_dir / "tcp-sharded-worker-overall-summary.txt"
if not summary_paths:
    overall_path.write_text("=== relayd tcp sharded worker overall summary ===\nno stream summaries available\n", encoding="utf-8")
    print(overall_path.read_text(encoding="utf-8"), end="")
    raise SystemExit(0)

UNITS_BPS = ['bps', 'kbps', 'mbps', 'gbps']
def format_decimal(value, units):
    value = float(value); unit_index = 0
    while value >= 1000.0 and unit_index < len(units) - 1:
        value /= 1000.0; unit_index += 1
    return f"{value:.2f} {units[unit_index]}"

summaries = [json.loads(path.read_text(encoding='utf-8')) for path in summary_paths]
lines = ["=== relayd tcp sharded worker overall summary ==="]
for item in summaries:
    lines.append(
        f"streams={item['streams']}: threaded {format_decimal(item['threaded_median_bps'], UNITS_BPS)}, "
        f"single-thread {format_decimal(item['session_model_median_bps'], UNITS_BPS)}, "
        f"workerized {format_decimal(item['workerized_median_bps'], UNITS_BPS)}, "
        f"sharded {format_decimal(item['sharded_median_bps'], UNITS_BPS)}, "
        f"s/w={('n/a' if item['sharded_vs_workerized_ratio'] is None else f'{item['sharded_vs_workerized_ratio']:.2f}x')}, "
        f"s/t={('n/a' if item['sharded_vs_threaded_ratio'] is None else f'{item['sharded_vs_threaded_ratio']:.2f}x')} "
        f"accept={item['sharded_accept_w0']}/{item['sharded_accept_w1']}/{item['sharded_accept_w2']}/{item['sharded_accept_w3']} "
        f"active_after={item['sharded_max_active_after']} -> {item['decision']}"
    )
overall_decision = (
    'sharded-worker tcp model is justified in current runtime architecture'
    if all(item['decision'] == 'sharded-worker tcp model is justified in current runtime architecture' for item in summaries)
    else 'sharded-worker tcp model did not clear the next gate in current runtime architecture'
)
lines.extend(['', overall_decision])
overall_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print(overall_path.read_text(encoding='utf-8'), end='')
PY
}

emit_tcp_accept_balanced_summary() {
  local results_file=$1
  local summary_txt=$2
  local summary_json=$3
  local direct_json=$4
  local direct_log=$5
  local direct_server_log=$6
  local threaded_json=$7
  local threaded_log=$8
  local threaded_server_log=$9
  local session_json=${10}
  local session_log=${11}
  local session_server_log=${12}
  local workerized_json=${13}
  local workerized_log=${14}
  local workerized_server_log=${15}
  local sharded_json=${16}
  local sharded_log=${17}
  local sharded_server_log=${18}
  local accept_json=${19}
  local accept_log=${20}
  local accept_server_log=${21}
  local legacy_tcp_json=${22}
  local legacy_tcp_log=${23}
  local legacy_tcp_server_log=${24}
  python3 - "$results_file" "$summary_txt" "$summary_json" "$direct_json" "$direct_log" "$direct_server_log" "$threaded_json" "$threaded_log" "$threaded_server_log" "$session_json" "$session_log" "$session_server_log" "$workerized_json" "$workerized_log" "$workerized_server_log" "$sharded_json" "$sharded_log" "$sharded_server_log" "$accept_json" "$accept_log" "$accept_server_log" "$legacy_tcp_json" "$legacy_tcp_log" "$legacy_tcp_server_log" <<'PY'
import json, pathlib, shutil, statistics, sys

results_path = pathlib.Path(sys.argv[1]); summary_path = pathlib.Path(sys.argv[2]); summary_json_path = pathlib.Path(sys.argv[3])
paths = [pathlib.Path(arg) for arg in sys.argv[4:25]]
records = [json.loads(line) for line in results_path.read_text(encoding='utf-8').splitlines() if line.strip()]
if not records:
    raise SystemExit("no TCP accept-balanced records found")

UNITS_BPS = ['bps', 'kbps', 'mbps', 'gbps']
def fmt(value):
    value = float(value); i = 0
    while value >= 1000.0 and i < len(UNITS_BPS) - 1:
        value /= 1000.0; i += 1
    return f"{value:.2f} {UNITS_BPS[i]}"

def summarize(items):
    bps = [item['bits_per_second'] for item in items]
    median_bps = statistics.median(bps)
    best_bps = max(bps)
    representative = min(items, key=lambda item: (abs(item['bits_per_second'] - median_bps), item['repetition']))
    return {
        'samples': len(items),
        'median_bps': median_bps,
        'best_bps': best_bps,
        'duration_sec': items[0]['duration_sec'],
        'streams': items[0]['streams'],
        'representative': representative,
        'handoff_total': sum(item.get('tcp_accept_handoff_delta', 0) for item in items),
        'handoff_w0': sum(item.get('tcp_accept_handoff_worker0_delta', 0) for item in items),
        'handoff_w1': sum(item.get('tcp_accept_handoff_worker1_delta', 0) for item in items),
        'handoff_w2': sum(item.get('tcp_accept_handoff_worker2_delta', 0) for item in items),
        'handoff_w3': sum(item.get('tcp_accept_handoff_worker3_delta', 0) for item in items),
        'accept_total': sum(item.get('tcp_listener_accept_delta', 0) for item in items),
        'accept_w0': sum(item.get('tcp_listener_accept_worker0_delta', 0) for item in items),
        'accept_w1': sum(item.get('tcp_listener_accept_worker1_delta', 0) for item in items),
        'accept_w2': sum(item.get('tcp_listener_accept_worker2_delta', 0) for item in items),
        'accept_w3': sum(item.get('tcp_listener_accept_worker3_delta', 0) for item in items),
        'connect_fail_total': sum(item.get('tcp_upstream_connect_fail_delta', 0) for item in items),
        'max_active_after': max(item.get('tcp_active_sessions_after', 0) for item in items),
    }

def copy_record(record, json_out, log_out, server_log_out):
    base = results_path.parent
    shutil.copy2(base / record['json_file'], json_out)
    shutil.copy2(base / record['client_log'], log_out)
    shutil.copy2(base / record['server_log'], server_log_out)

grouped = {}
for record in records:
    grouped.setdefault(record['path'], []).append(record)

direct = summarize(sorted(grouped.get('direct', []), key=lambda item: item['repetition']))
threaded = summarize(sorted(grouped.get('threaded', []), key=lambda item: item['repetition']))
session = summarize(sorted(grouped.get('session-model', []), key=lambda item: item['repetition']))
workerized = summarize(sorted(grouped.get('workerized-session', []), key=lambda item: item['repetition']))
sharded = summarize(sorted(grouped.get('sharded-worker', []), key=lambda item: item['repetition']))
accept_bal = summarize(sorted(grouped.get('accept-balanced', []), key=lambda item: item['repetition']))
if any(item['samples'] == 0 for item in (direct, threaded, session, workerized, sharded, accept_bal)):
    raise SystemExit("expected direct, threaded, session-model, workerized-session, sharded-worker, and accept-balanced records")

copy_record(direct['representative'], paths[0], paths[1], paths[2])
copy_record(threaded['representative'], paths[3], paths[4], paths[5])
copy_record(session['representative'], paths[6], paths[7], paths[8])
copy_record(workerized['representative'], paths[9], paths[10], paths[11])
copy_record(sharded['representative'], paths[12], paths[13], paths[14])
copy_record(accept_bal['representative'], paths[15], paths[16], paths[17])
shutil.copy2(paths[15], paths[18]); shutil.copy2(paths[16], paths[19]); shutil.copy2(paths[17], paths[20])

ab_vs_sharded = accept_bal['median_bps'] / sharded['median_bps'] if sharded['median_bps'] else None
ab_vs_threaded = accept_bal['median_bps'] / threaded['median_bps'] if threaded['median_bps'] else None
ab_vs_workerized = accept_bal['median_bps'] / workerized['median_bps'] if workerized['median_bps'] else None
streams = direct['streams']
decision = 'accept-balanced tcp model did not clear the next gate in current runtime architecture'
if (
    accept_bal['max_active_after'] == 0 and accept_bal['connect_fail_total'] == 0
    and accept_bal['handoff_w0'] > 0 and accept_bal['handoff_w1'] > 0 and accept_bal['handoff_w2'] > 0 and accept_bal['handoff_w3'] > 0
    and ab_vs_sharded is not None and ab_vs_threaded is not None
    and ((streams == 1 and ab_vs_sharded >= 0.95) or (streams != 1 and ab_vs_sharded >= 1.10 and ab_vs_threaded >= 0.85))
):
    decision = 'accept-balanced tcp model is justified in current runtime architecture'

summary = {
    'streams': streams,
    'repetitions': direct['samples'],
    'duration_sec': direct['duration_sec'],
    'decision': decision,
    'direct_median_bps': direct['median_bps'],
    'threaded_median_bps': threaded['median_bps'],
    'session_model_median_bps': session['median_bps'],
    'workerized_median_bps': workerized['median_bps'],
    'sharded_median_bps': sharded['median_bps'],
    'accept_balanced_median_bps': accept_bal['median_bps'],
    'accept_balanced_vs_sharded_ratio': ab_vs_sharded,
    'accept_balanced_vs_threaded_ratio': ab_vs_threaded,
    'accept_balanced_vs_workerized_ratio': ab_vs_workerized,
    'accept_balanced_handoff_total': accept_bal['handoff_total'],
    'accept_balanced_handoff_w0': accept_bal['handoff_w0'],
    'accept_balanced_handoff_w1': accept_bal['handoff_w1'],
    'accept_balanced_handoff_w2': accept_bal['handoff_w2'],
    'accept_balanced_handoff_w3': accept_bal['handoff_w3'],
    'accept_balanced_max_active_after': accept_bal['max_active_after'],
    'accept_balanced_connect_fail_total': accept_bal['connect_fail_total'],
}
summary_json_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n', encoding='utf-8')

def ratio_text(v): return 'n/a' if v is None else f"{v:.2f}x"
lines = [
    '=== relayd tcp accept-balanced summary ===',
    f"repetitions: {direct['samples']}",
    f"duration:    {direct['duration_sec']:g}s",
    f"streams:     {streams}",
    '',
    f"direct throughput:                 {fmt(direct['median_bps'])} median / {fmt(direct['best_bps'])} best",
    f"threaded throughput:               {fmt(threaded['median_bps'])} median / {fmt(threaded['best_bps'])} best",
    f"single-thread session throughput:  {fmt(session['median_bps'])} median / {fmt(session['best_bps'])} best",
    f"workerized throughput:             {fmt(workerized['median_bps'])} median / {fmt(workerized['best_bps'])} best",
    f"sharded throughput:                {fmt(sharded['median_bps'])} median / {fmt(sharded['best_bps'])} best",
    f"accept-balanced throughput:        {fmt(accept_bal['median_bps'])} median / {fmt(accept_bal['best_bps'])} best",
    f"accept-balanced/sharded ratio: {ratio_text(ab_vs_sharded)}",
    f"accept-balanced/threaded ratio: {ratio_text(ab_vs_threaded)}",
    f"accept-balanced/workerized ratio: {ratio_text(ab_vs_workerized)}",
    f"v4 sharded accept note: total={sharded['accept_total']} w0={sharded['accept_w0']} w1={sharded['accept_w1']} w2={sharded['accept_w2']} w3={sharded['accept_w3']}",
    f"v5 handoff note: total={accept_bal['handoff_total']} w0={accept_bal['handoff_w0']} w1={accept_bal['handoff_w1']} w2={accept_bal['handoff_w2']} w3={accept_bal['handoff_w3']}",
    f"accept-balanced active-after note: max active sessions after run = {accept_bal['max_active_after']}",
    decision,
]
summary_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print(summary_path.read_text(encoding='utf-8'), end='')
PY
}

refresh_tcp_accept_balanced_overall_summary() {
  local latest_dir=$1
  python3 - "$latest_dir" <<'PY'
import json, pathlib, sys

latest_dir = pathlib.Path(sys.argv[1])
summary_paths = sorted(latest_dir.glob("tcp-accept-balanced-streams-*-summary.json"))
overall_path = latest_dir / "tcp-accept-balanced-overall-summary.txt"
if not summary_paths:
    overall_path.write_text("=== relayd tcp accept-balanced overall summary ===\nno stream summaries available\n", encoding='utf-8')
    print(overall_path.read_text(encoding='utf-8'), end='')
    raise SystemExit(0)

UNITS = ['bps','kbps','mbps','gbps']
def fmt(v):
    v=float(v);i=0
    while v>=1000.0 and i<len(UNITS)-1:
        v/=1000.0;i+=1
    return f"{v:.2f} {UNITS[i]}"

summaries=[json.loads(p.read_text(encoding='utf-8')) for p in summary_paths]
lines=["=== relayd tcp accept-balanced overall summary ==="]
for item in summaries:
    lines.append(
        f"streams={item['streams']}: threaded {fmt(item['threaded_median_bps'])}, "
        f"workerized {fmt(item['workerized_median_bps'])}, sharded {fmt(item['sharded_median_bps'])}, "
        f"accept-balanced {fmt(item['accept_balanced_median_bps'])}, "
        f"ab/sharded={('n/a' if item['accept_balanced_vs_sharded_ratio'] is None else f'{item['accept_balanced_vs_sharded_ratio']:.2f}x')}, "
        f"ab/threaded={('n/a' if item['accept_balanced_vs_threaded_ratio'] is None else f'{item['accept_balanced_vs_threaded_ratio']:.2f}x')} "
        f"handoff={item['accept_balanced_handoff_w0']}/{item['accept_balanced_handoff_w1']}/{item['accept_balanced_handoff_w2']}/{item['accept_balanced_handoff_w3']} "
        f"active_after={item['accept_balanced_max_active_after']} -> {item['decision']}"
    )
overall_decision = (
    'accept-balanced tcp model is justified in current runtime architecture'
    if all(item['decision'] == 'accept-balanced tcp model is justified in current runtime architecture' for item in summaries)
    else 'accept-balanced tcp model did not clear the next gate in current runtime architecture'
)
lines.extend(['', overall_decision])
overall_path.write_text('\n'.join(lines)+'\n', encoding='utf-8')
print(overall_path.read_text(encoding='utf-8'), end='')
PY
}

emit_matrix_report() {
  local results_file=$1
  local summary_txt=$2
  local summary_json=$3
  local rates_csv=$4
  local packet_sizes_csv=$5
  local repetitions=$6
  local duration=$7
  python3 - "$results_file" "$summary_txt" "$summary_json" "$rates_csv" "$packet_sizes_csv" "$repetitions" "$duration" <<'PY'
import json
import pathlib
import statistics
import sys

results_path = pathlib.Path(sys.argv[1])
summary_txt_path = pathlib.Path(sys.argv[2])
summary_json_path = pathlib.Path(sys.argv[3])
rates = [item.strip() for item in sys.argv[4].split(',') if item.strip()]
packet_sizes = [int(item.strip()) for item in sys.argv[5].split(',') if item.strip()]
repetitions = int(sys.argv[6])
duration = float(sys.argv[7])
records = [json.loads(line) for line in results_path.read_text(encoding='utf-8').splitlines() if line.strip()]

UNITS_BPS = ['bps', 'kbps', 'mbps', 'gbps']
UNITS_BYTES = ['bytes', 'kbytes', 'mbytes', 'gbytes']

def format_decimal(value, units):
    value = float(value)
    unit_index = 0
    while value >= 1000.0 and unit_index < len(units) - 1:
        value /= 1000.0
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"

def aggregate(items):
    throughputs = [item['bits_per_second'] for item in items]
    losses = [item['lost_percent'] for item in items]
    jitters = [item['jitter_ms'] for item in items]
    transfers = [item['bytes'] for item in items]
    return {
        'samples': len(items),
        'median_bps': statistics.median(throughputs),
        'best_bps': max(throughputs),
        'median_loss_percent': statistics.median(losses),
        'best_loss_percent': min(losses),
        'median_jitter_ms': statistics.median(jitters),
        'best_jitter_ms': min(jitters),
        'median_bytes': statistics.median(transfers),
        'best_bytes': max(transfers),
    }

def ratio(relay, direct):
    return relay / direct if direct else None

by_key = {}
for record in records:
    key = (record['rate'], record['packet_size'], record['path'])
    by_key.setdefault(key, []).append(record)

pairs = []
for rate in rates:
    for packet_size in packet_sizes:
        direct_records = by_key.get((rate, packet_size, 'direct'), [])
        relay_records = by_key.get((rate, packet_size, 'relay'), [])
        if not direct_records or not relay_records:
            continue
        direct = aggregate(direct_records)
        relay = aggregate(relay_records)
        pair = {
            'rate': rate,
            'packet_size': packet_size,
            'repetitions': repetitions,
            'duration_sec': duration,
            'direct': direct,
            'relay': relay,
            'relay_to_direct_median_bps_ratio': ratio(relay['median_bps'], direct['median_bps']),
            'relay_to_direct_best_bps_ratio': ratio(relay['best_bps'], direct['best_bps']),
            'loss_delta_percent_points': relay['median_loss_percent'] - direct['median_loss_percent'],
        }
        pairs.append(pair)

summary = {
    'rates': rates,
    'packet_sizes': packet_sizes,
    'repetitions': repetitions,
    'duration_sec': duration,
    'runs': records,
    'pairs': pairs,
    'focus_points': {},
}
for rate, packet_size in [('10G', 1472), ('25G', 1472)]:
    for pair in pairs:
        if pair['rate'] == rate and pair['packet_size'] == packet_size:
            summary['focus_points'][f'{rate}/{packet_size}'] = pair
            break

summary_json_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding='utf-8')

lines = [
    '=== relayd udp matrix summary ===',
    f'repetitions: {repetitions}',
    f'duration:    {duration:g}s',
    f'runs:        {len(records)}',
    '',
    'rate  pkt   direct median/best      relay median/best       relay/direct   direct loss  relay loss  delta',
]
for pair in pairs:
    ratio_value = pair['relay_to_direct_median_bps_ratio']
    ratio_text = 'n/a' if ratio_value is None else f"{ratio_value:.2f}x"
    delta = pair['loss_delta_percent_points']
    delta_text = f"{delta:+.2f} pp"
    lines.append(
        f"{pair['rate']:<5} {pair['packet_size']:<5} "
        f"{format_decimal(pair['direct']['median_bps'], UNITS_BPS):>18} / {format_decimal(pair['direct']['best_bps'], UNITS_BPS):<18} "
        f"{format_decimal(pair['relay']['median_bps'], UNITS_BPS):>18} / {format_decimal(pair['relay']['best_bps'], UNITS_BPS):<18} "
        f"{ratio_text:>8} "
        f"{pair['direct']['median_loss_percent']:>10.2f}% "
        f"{pair['relay']['median_loss_percent']:>10.2f}% "
        f"{delta_text:>10}"
    )

if summary['focus_points']:
    lines.extend(['', 'focus points:'])
    for label, pair in summary['focus_points'].items():
        lines.append(
            f"- {label}: relay median {format_decimal(pair['relay']['median_bps'], UNITS_BPS)}, "
            f"relay best {format_decimal(pair['relay']['best_bps'], UNITS_BPS)}, "
            f"median loss {pair['relay']['median_loss_percent']:.2f}%, "
            f"ratio vs direct {pair['relay_to_direct_median_bps_ratio']:.2f}x"
            if pair['relay_to_direct_median_bps_ratio'] is not None else
            f"- {label}: relay median {format_decimal(pair['relay']['median_bps'], UNITS_BPS)}, "
            f"relay best {format_decimal(pair['relay']['best_bps'], UNITS_BPS)}, "
            f"median loss {pair['relay']['median_loss_percent']:.2f}%"
        )

summary_txt_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print(summary_txt_path.read_text(encoding='utf-8'), end='')
PY
}

publish_artifacts() {
  python3 - "$RUN_DIR" "$LATEST_RUN_DIR" <<'PY'
import pathlib
import shutil
import sys

src = pathlib.Path(sys.argv[1])
dst = pathlib.Path(sys.argv[2])
dst.parent.mkdir(parents=True, exist_ok=True)
shutil.copytree(src, dst, dirs_exist_ok=True)
PY
}

write_manifest() {
  cat >"$RUN_MANIFEST" <<EOF_MANIFEST
IPERF_MODE=${IPERF_MODE}
IPERF_DURATION=${IPERF_DURATION}
UDP_MATRIX_DURATION=${UDP_MATRIX_DURATION}
UDP_RATE=${UDP_RATE}
UDP_PACKET_SIZE=${UDP_PACKET_SIZE}
TCP_SESSION_MODEL_ENABLED=${TCP_SESSION_MODEL_ENABLED}
TCP_SESSION_MODEL_WORKERS=${TCP_SESSION_MODEL_WORKERS}
TCP_SESSION_MODEL_ACCEPT_BALANCED=${TCP_SESSION_MODEL_ACCEPT_BALANCED}
TCP_SESSION_MODEL_SHARDED_ACCEPT=${TCP_SESSION_MODEL_SHARDED_ACCEPT}
TCP_SPLICE_ENABLED=${TCP_SPLICE_ENABLED}
FORCE_TCP_COPY_FALLBACK=${FORCE_TCP_COPY_FALLBACK}
TCP_COMPARE_MODE=${TCP_COMPARE_MODE}
UDP_SWEEP_RATES=${UDP_SWEEP_RATES}
UDP_PACKET_SIZES=${UDP_PACKET_SIZES}
IPERF_REPETITIONS=${IPERF_REPETITIONS}
TCP_DIRECT_VS_RELAY=${TCP_DIRECT_VS_RELAY}
TCP_BENCH_DURATION=${TCP_BENCH_DURATION}
TCP_BENCH_REPETITIONS=${TCP_BENCH_REPETITIONS}
TCP_STREAMS=${TCP_STREAMS}
RUN_DIR=${RUN_DIR}
LATEST_RUN_DIR=${LATEST_RUN_DIR}
EOF_MANIFEST
}

run_iperf_udp_client() {
  local target_port=$1
  local rate=$2
  local duration=$3
  local packet_size=$4
  local output_json=$5
  local output_log=$6
  local -a args

  args=(-c "$TARGET_HOST" -p "$target_port" -u -b "$rate" -t "$duration" -J)
  if [[ -n "$packet_size" ]]; then
    args+=(-l "$packet_size")
  fi

  "$IPERF3_BIN" "${args[@]}" >"$output_json" 2>"$output_log"
}

run_iperf_tcp_client() {
  local target_port=$1
  local duration=$2
  local streams=$3
  local output_json=$4
  local output_log=$5
  local -a args

  args=(-c "$TARGET_HOST" -p "$target_port" -t "$duration" -P "$streams" -J)
  "$IPERF3_BIN" "${args[@]}" >"$output_json" 2>"$output_log"
}

current_tcp_results_file() {
  case "$TCP_COMPARE_MODE" in
    accept-balanced) printf '%s\n' "$TCP_ACCEPT_BALANCED_RESULTS" ;;
    copy-vs-splice) printf '%s\n' "$TCP_COPY_VS_SPLICE_RESULTS" ;;
    session-model) printf '%s\n' "$TCP_SESSION_MODEL_RESULTS" ;;
    workerized-session) printf '%s\n' "$TCP_WORKERIZED_SESSION_RESULTS" ;;
    sharded-worker) printf '%s\n' "$TCP_SHARDED_WORKER_RESULTS" ;;
  esac
}

run_tcp_relay_smoke() {
  tcp_target_port=$(pick_free_port tcp)
  log "starting tcp iperf3 server on ${TARGET_HOST}:${tcp_target_port}"
  "$IPERF3_BIN" -s -1 -B "$TARGET_HOST" -p "$tcp_target_port" >"$TCP_SERVER_LOG" 2>&1 &
  tcp_server_pid=$!
  track_child "$tcp_server_pid"
  wait_for_tcp_listener "$TARGET_HOST" "$tcp_target_port" "$IPERF_SERVER_READY_TIMEOUT_SEC" || die "tcp iperf3 server did not become ready on ${TARGET_HOST}:${tcp_target_port}"

  create_allocation tcp "$tcp_target_port"
  tcp_allocation_id=$CREATED_ALLOCATION_ID
  tcp_relay_port=$CREATED_RELAY_PORT
  log "tcp allocation id=${tcp_allocation_id} relay_port=${tcp_relay_port}"

  run_iperf_tcp_client "$tcp_relay_port" "$IPERF_DURATION" "$TCP_STREAMS" "$TCP_CLIENT_JSON" "$TCP_CLIENT_LOG"
  log "tcp $(assert_iperf_positive "$TCP_CLIENT_JSON" tcp)"
  wait "$tcp_server_pid"
  delete_allocation "$tcp_allocation_id"
}

run_udp_relay_smoke() {
  udp_target_port=$(pick_free_port tcp)
  log "starting udp iperf3 server on ${TARGET_HOST}:${udp_target_port} (requires tcp control + udp data on the same relay port)"
  "$IPERF3_BIN" -s -1 -B "$TARGET_HOST" -p "$udp_target_port" >"$UDP_SERVER_LOG" 2>&1 &
  udp_server_pid=$!
  track_child "$udp_server_pid"
  wait_for_tcp_listener "$TARGET_HOST" "$udp_target_port" "$IPERF_SERVER_READY_TIMEOUT_SEC" || die "udp iperf3 server did not become ready on ${TARGET_HOST}:${udp_target_port}"

  create_allocation tcp "$udp_target_port"
  udp_control_allocation_id=$CREATED_ALLOCATION_ID
  udp_control_relay_port=$CREATED_RELAY_PORT
  log "udp control allocation id=${udp_control_allocation_id} relay_port=${udp_control_relay_port}"

  create_allocation udp "$udp_target_port"
  udp_allocation_id=$CREATED_ALLOCATION_ID
  udp_relay_port=$CREATED_RELAY_PORT
  log "udp allocation id=${udp_allocation_id} relay_port=${udp_relay_port}"
  [[ "$udp_control_relay_port" == "$udp_relay_port" ]] || die "iperf3 UDP requires matching TCP control and UDP relay ports, got tcp=${udp_control_relay_port} udp=${udp_relay_port}"

  run_iperf_udp_client "$udp_relay_port" "$UDP_RATE" "$IPERF_DURATION" "$UDP_PACKET_SIZE" "$UDP_CLIENT_JSON" "$UDP_CLIENT_LOG"
  log "udp $(assert_iperf_positive "$UDP_CLIENT_JSON" udp)"
  if udp_loss=$(udp_lost_percent "$UDP_CLIENT_JSON"); then
    [[ -n "$udp_loss" ]] && log "udp lost_percent=${udp_loss} (non-gating)"
  fi
  wait "$udp_server_pid"
  delete_allocation "$udp_control_allocation_id"
  delete_allocation "$udp_allocation_id"
}

run_one_shot_suite() {
  local report
  if [[ "$TCP_DIRECT_VS_RELAY" == "1" ]]; then
    case "$TCP_COMPARE_MODE" in
      accept-balanced) run_tcp_accept_balanced_suite ;;
      sharded-worker) run_tcp_sharded_worker_suite ;;
      copy-vs-splice) run_tcp_copy_vs_splice_suite ;;
      session-model) run_tcp_session_model_suite ;;
      workerized-session) run_tcp_workerized_session_suite ;;
    esac
  else
    start_relayd "$TCP_SPLICE_ENABLED" "$FORCE_TCP_COPY_FALLBACK" tcp-relay-smoke "$TCP_SESSION_MODEL_ENABLED" "$TCP_SESSION_MODEL_WORKERS" "$TCP_SESSION_MODEL_SHARDED_ACCEPT" "$TCP_SESSION_MODEL_ACCEPT_BALANCED"
    run_tcp_relay_smoke
    stop_relayd
  fi
  start_relayd "$TCP_SPLICE_ENABLED" "$FORCE_TCP_COPY_FALLBACK" udp-relay-smoke "$TCP_SESSION_MODEL_ENABLED" "$TCP_SESSION_MODEL_WORKERS" "$TCP_SESSION_MODEL_SHARDED_ACCEPT" "$TCP_SESSION_MODEL_ACCEPT_BALANCED"
  run_udp_relay_smoke
  stop_relayd
  report=$(emit_stdout_report "$TCP_CLIENT_JSON" "$UDP_CLIENT_JSON" "$UDP_RATE" "$UDP_PACKET_SIZE")
  printf '%s\n' "$report" | tee "$ONE_SHOT_REPORT"
}

run_tcp_direct_trial() {
  local repetition=$1
  local duration=$2
  local streams=$3
  local output_prefix=$4
  local target_port server_log client_json client_log server_pid

  target_port=$(pick_free_port tcp)
  server_log="${output_prefix}-server.log"
  client_json="${output_prefix}-client.json"
  client_log="${output_prefix}-client.log"

  log "tcp direct trial repetition=${repetition} streams=${streams} target_port=${target_port}"
  "$IPERF3_BIN" -s -1 -B "$TARGET_HOST" -p "$target_port" >"$server_log" 2>&1 &
  server_pid=$!
  track_child "$server_pid"
  wait_for_tcp_listener "$TARGET_HOST" "$target_port" "$IPERF_SERVER_READY_TIMEOUT_SEC" || die "tcp direct iperf3 server did not become ready on ${TARGET_HOST}:${target_port}"

  run_iperf_tcp_client "$target_port" "$duration" "$streams" "$client_json" "$client_log"
  log "tcp direct repetition=${repetition} $(assert_iperf_positive "$client_json" tcp)"
  wait "$server_pid"
  cat >"${output_prefix}-metrics-before.json" <<'EOF_METRICS'
{}
EOF_METRICS
  cat >"${output_prefix}-metrics-after.json" <<'EOF_METRICS'
{}
EOF_METRICS
  append_tcp_result "$(current_tcp_results_file)" direct "$repetition" "$duration" "$streams" "$client_json" "$client_log" "$server_log" "${output_prefix}-metrics-before.json" "${output_prefix}-metrics-after.json"
}

run_tcp_relay_trial() {
  local repetition=$1
  local duration=$2
  local streams=$3
  local output_prefix=$4
  local path_label=$5
  local target_port server_log client_json client_log server_pid
  local allocation_id relay_port
  local before_metrics after_metrics

  target_port=$(pick_free_port tcp)
  server_log="${output_prefix}-server.log"
  client_json="${output_prefix}-client.json"
  client_log="${output_prefix}-client.log"
  before_metrics="${output_prefix}-metrics-before.json"
  after_metrics="${output_prefix}-metrics-after.json"

  log "tcp ${path_label} trial repetition=${repetition} streams=${streams} target_port=${target_port}"
  "$IPERF3_BIN" -s -1 -B "$TARGET_HOST" -p "$target_port" >"$server_log" 2>&1 &
  server_pid=$!
  track_child "$server_pid"
  wait_for_tcp_listener "$TARGET_HOST" "$target_port" "$IPERF_SERVER_READY_TIMEOUT_SEC" || die "tcp ${path_label} iperf3 server did not become ready on ${TARGET_HOST}:${target_port}"

  create_allocation tcp "$target_port"
  allocation_id=$CREATED_ALLOCATION_ID
  relay_port=$CREATED_RELAY_PORT
  capture_metrics_snapshot "$before_metrics"

  run_iperf_tcp_client "$relay_port" "$duration" "$streams" "$client_json" "$client_log"
  log "tcp ${path_label} repetition=${repetition} relay_port=${relay_port} $(assert_iperf_positive "$client_json" tcp)"
  wait "$server_pid"
  sleep 0.1
  capture_metrics_snapshot "$after_metrics"
  delete_allocation "$allocation_id"
  append_tcp_result "$(current_tcp_results_file)" "$path_label" "$repetition" "$duration" "$streams" "$client_json" "$client_log" "$server_log" "$before_metrics" "$after_metrics"
}

run_tcp_copy_vs_splice_suite() {
  local repetition trial_dir output_prefix

  mkdir -p "$TCP_COPY_VS_SPLICE_DIR"
  : >"$TCP_COPY_VS_SPLICE_RESULTS"

  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_COPY_VS_SPLICE_DIR}/rep-${repetition}"
    mkdir -p "$trial_dir"
    output_prefix="${trial_dir}/direct"
    run_tcp_direct_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix"
  done

  start_relayd 0 1 tcp-copy 0 0 0 0
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_COPY_VS_SPLICE_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/copy"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" copy
  done

  start_relayd 1 0 tcp-splice 0 0 0 0
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_COPY_VS_SPLICE_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/splice"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" splice
  done
  stop_relayd

  emit_tcp_copy_vs_splice_summary \
    "$TCP_COPY_VS_SPLICE_RESULTS" \
    "$TCP_COPY_VS_SPLICE_SUMMARY_TXT" \
    "$TCP_COPY_VS_SPLICE_SUMMARY_JSON" \
    "$TCP_DIRECT_JSON" \
    "$TCP_DIRECT_LOG" \
    "$TCP_DIRECT_SERVER_LOG" \
    "$TCP_COPY_JSON" \
    "$TCP_COPY_LOG" \
    "$TCP_COPY_SERVER_LOG" \
    "$TCP_SPLICE_JSON" \
    "$TCP_SPLICE_LOG" \
    "$TCP_SPLICE_SERVER_LOG" \
    "$TCP_CLIENT_JSON" \
    "$TCP_CLIENT_LOG" \
    "$TCP_SERVER_LOG"
}

run_tcp_session_model_suite() {
  local repetition trial_dir output_prefix

  mkdir -p "$TCP_SESSION_MODEL_DIR"
  : >"$TCP_SESSION_MODEL_RESULTS"

  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_SESSION_MODEL_DIR}/rep-${repetition}"
    mkdir -p "$trial_dir"
    output_prefix="${trial_dir}/direct"
    run_tcp_direct_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix"
  done

  start_relayd 0 0 tcp-threaded 0 0 0 0
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_SESSION_MODEL_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/threaded"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" threaded
  done

  start_relayd 0 0 tcp-session-model 1 0 0 0
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_SESSION_MODEL_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/session-model"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" session-model
  done
  stop_relayd

  emit_tcp_session_model_summary \
    "$TCP_SESSION_MODEL_RESULTS" \
    "$TCP_SESSION_MODEL_SUMMARY_TXT" \
    "$TCP_SESSION_MODEL_SUMMARY_JSON" \
    "$TCP_DIRECT_JSON" \
    "$TCP_DIRECT_LOG" \
    "$TCP_DIRECT_SERVER_LOG" \
    "$TCP_THREADED_JSON" \
    "$TCP_THREADED_LOG" \
    "$TCP_THREADED_SERVER_LOG" \
    "$TCP_SESSION_MODEL_JSON" \
    "$TCP_SESSION_MODEL_LOG" \
    "$TCP_SESSION_MODEL_SERVER_LOG" \
    "$TCP_CLIENT_JSON" \
    "$TCP_CLIENT_LOG" \
    "$TCP_SERVER_LOG"
}

run_tcp_workerized_session_suite() {
  local repetition trial_dir output_prefix

  mkdir -p "$TCP_WORKERIZED_SESSION_DIR"
  : >"$TCP_WORKERIZED_SESSION_RESULTS"

  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_WORKERIZED_SESSION_DIR}/rep-${repetition}"
    mkdir -p "$trial_dir"
    output_prefix="${trial_dir}/direct"
    run_tcp_direct_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix"
  done

  start_relayd 0 0 tcp-threaded 0 0 0 0
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_WORKERIZED_SESSION_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/threaded"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" threaded
  done

  start_relayd 0 0 tcp-session-model 1 0 0 0
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_WORKERIZED_SESSION_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/session-model"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" session-model
  done

  start_relayd 0 0 tcp-workerized-session 1 2 0 0
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_WORKERIZED_SESSION_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/workerized-session"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" workerized-session
  done
  stop_relayd

  emit_tcp_workerized_session_summary \
    "$TCP_WORKERIZED_SESSION_RESULTS" \
    "$TCP_WORKERIZED_SESSION_SUMMARY_TXT" \
    "$TCP_WORKERIZED_SESSION_SUMMARY_JSON" \
    "$TCP_DIRECT_JSON" \
    "$TCP_DIRECT_LOG" \
    "$TCP_DIRECT_SERVER_LOG" \
    "$TCP_THREADED_JSON" \
    "$TCP_THREADED_LOG" \
    "$TCP_THREADED_SERVER_LOG" \
    "$TCP_SESSION_MODEL_JSON" \
    "$TCP_SESSION_MODEL_LOG" \
    "$TCP_SESSION_MODEL_SERVER_LOG" \
    "$TCP_WORKERIZED_SESSION_JSON" \
    "$TCP_WORKERIZED_SESSION_LOG" \
    "$TCP_WORKERIZED_SESSION_SERVER_LOG" \
    "$TCP_CLIENT_JSON" \
    "$TCP_CLIENT_LOG" \
    "$TCP_SERVER_LOG"
}

run_tcp_sharded_worker_suite() {
  local repetition trial_dir output_prefix

  mkdir -p "$TCP_SHARDED_WORKER_DIR"
  : >"$TCP_SHARDED_WORKER_RESULTS"

  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_SHARDED_WORKER_DIR}/rep-${repetition}"
    mkdir -p "$trial_dir"
    output_prefix="${trial_dir}/direct"
    run_tcp_direct_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix"
  done

  start_relayd 0 0 tcp-threaded 0 0 0 0
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_SHARDED_WORKER_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/threaded"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" threaded
  done

  start_relayd 0 0 tcp-session-model 1 0 0 0
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_SHARDED_WORKER_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/session-model"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" session-model
  done

  start_relayd 0 0 tcp-workerized-session 1 2 0 0
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_SHARDED_WORKER_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/workerized-session"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" workerized-session
  done

  start_relayd 0 0 tcp-sharded-worker 1 4 1 0
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_SHARDED_WORKER_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/sharded-worker"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" sharded-worker
  done
  stop_relayd

  emit_tcp_sharded_worker_summary \
    "$TCP_SHARDED_WORKER_RESULTS" \
    "$TCP_SHARDED_WORKER_SUMMARY_TXT" \
    "$TCP_SHARDED_WORKER_SUMMARY_JSON" \
    "$TCP_DIRECT_JSON" \
    "$TCP_DIRECT_LOG" \
    "$TCP_DIRECT_SERVER_LOG" \
    "$TCP_THREADED_JSON" \
    "$TCP_THREADED_LOG" \
    "$TCP_THREADED_SERVER_LOG" \
    "$TCP_SESSION_MODEL_JSON" \
    "$TCP_SESSION_MODEL_LOG" \
    "$TCP_SESSION_MODEL_SERVER_LOG" \
    "$TCP_WORKERIZED_SESSION_JSON" \
    "$TCP_WORKERIZED_SESSION_LOG" \
    "$TCP_WORKERIZED_SESSION_SERVER_LOG" \
    "$TCP_SHARDED_WORKER_JSON" \
    "$TCP_SHARDED_WORKER_LOG" \
    "$TCP_SHARDED_WORKER_SERVER_LOG" \
    "$TCP_CLIENT_JSON" \
    "$TCP_CLIENT_LOG" \
    "$TCP_SERVER_LOG"
}

run_tcp_accept_balanced_suite() {
  local repetition trial_dir output_prefix

  mkdir -p "$TCP_ACCEPT_BALANCED_DIR"
  : >"$TCP_ACCEPT_BALANCED_RESULTS"

  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_ACCEPT_BALANCED_DIR}/rep-${repetition}"
    mkdir -p "$trial_dir"
    output_prefix="${trial_dir}/direct"
    run_tcp_direct_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix"
  done

  start_relayd 0 0 tcp-threaded 0 0 0 0
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_ACCEPT_BALANCED_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/threaded"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" threaded
  done

  start_relayd 0 0 tcp-session-model 1 0 0 0
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_ACCEPT_BALANCED_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/session-model"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" session-model
  done

  start_relayd 0 0 tcp-workerized-session 1 2 0 0
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_ACCEPT_BALANCED_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/workerized-session"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" workerized-session
  done

  start_relayd 0 0 tcp-sharded-worker 1 4 1 0
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_ACCEPT_BALANCED_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/sharded-worker"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" sharded-worker
  done

  start_relayd 0 0 tcp-accept-balanced 1 4 0 1
  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_ACCEPT_BALANCED_DIR}/rep-${repetition}"
    output_prefix="${trial_dir}/accept-balanced"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix" accept-balanced
  done
  stop_relayd

  emit_tcp_accept_balanced_summary \
    "$TCP_ACCEPT_BALANCED_RESULTS" \
    "$TCP_ACCEPT_BALANCED_SUMMARY_TXT" \
    "$TCP_ACCEPT_BALANCED_SUMMARY_JSON" \
    "$TCP_DIRECT_JSON" \
    "$TCP_DIRECT_LOG" \
    "$TCP_DIRECT_SERVER_LOG" \
    "$TCP_THREADED_JSON" \
    "$TCP_THREADED_LOG" \
    "$TCP_THREADED_SERVER_LOG" \
    "$TCP_SESSION_MODEL_JSON" \
    "$TCP_SESSION_MODEL_LOG" \
    "$TCP_SESSION_MODEL_SERVER_LOG" \
    "$TCP_WORKERIZED_SESSION_JSON" \
    "$TCP_WORKERIZED_SESSION_LOG" \
    "$TCP_WORKERIZED_SESSION_SERVER_LOG" \
    "$TCP_SHARDED_WORKER_JSON" \
    "$TCP_SHARDED_WORKER_LOG" \
    "$TCP_SHARDED_WORKER_SERVER_LOG" \
    "$TCP_ACCEPT_BALANCED_JSON" \
    "$TCP_ACCEPT_BALANCED_LOG" \
    "$TCP_ACCEPT_BALANCED_SERVER_LOG" \
    "$TCP_CLIENT_JSON" \
    "$TCP_CLIENT_LOG" \
    "$TCP_SERVER_LOG"
}

run_udp_direct_trial() {
  local rate=$1
  local packet_size=$2
  local repetition=$3
  local duration=$4
  local output_prefix=$5
  local target_port server_log client_json client_log server_pid

  target_port=$(pick_free_port tcp)
  server_log="${output_prefix}-server.log"
  client_json="${output_prefix}-client.json"
  client_log="${output_prefix}-client.log"

  log "udp direct trial rate=${rate} packet_size=${packet_size} repetition=${repetition} target_port=${target_port}"
  "$IPERF3_BIN" -s -1 -B "$TARGET_HOST" -p "$target_port" >"$server_log" 2>&1 &
  server_pid=$!
  track_child "$server_pid"
  wait_for_tcp_listener "$TARGET_HOST" "$target_port" "$IPERF_SERVER_READY_TIMEOUT_SEC" || die "udp direct iperf3 server did not become ready on ${TARGET_HOST}:${target_port}"

  if ! run_iperf_udp_client "$target_port" "$rate" "$duration" "$packet_size" "$client_json" "$client_log"; then
    wait "$server_pid" 2>/dev/null || true
    return 1
  fi

  log "udp direct rate=${rate} packet_size=${packet_size} repetition=${repetition} $(assert_iperf_positive "$client_json" udp)"
  if udp_loss=$(udp_lost_percent "$client_json"); then
    [[ -n "$udp_loss" ]] && log "udp direct rate=${rate} packet_size=${packet_size} repetition=${repetition} lost_percent=${udp_loss}"
  fi
  wait "$server_pid"
  append_udp_result "$UDP_MATRIX_RESULTS" direct "$rate" "$packet_size" "$repetition" "$duration" "$client_json" "$client_log" "$server_log"
}

run_udp_relay_trial() {
  local rate=$1
  local packet_size=$2
  local repetition=$3
  local duration=$4
  local output_prefix=$5
  local target_port server_log client_json client_log server_pid
  local control_id control_port relay_id relay_port

  target_port=$(pick_free_port tcp)
  server_log="${output_prefix}-server.log"
  client_json="${output_prefix}-client.json"
  client_log="${output_prefix}-client.log"

  log "udp relay trial rate=${rate} packet_size=${packet_size} repetition=${repetition} target_port=${target_port}"
  "$IPERF3_BIN" -s -1 -B "$TARGET_HOST" -p "$target_port" >"$server_log" 2>&1 &
  server_pid=$!
  track_child "$server_pid"
  wait_for_tcp_listener "$TARGET_HOST" "$target_port" "$IPERF_SERVER_READY_TIMEOUT_SEC" || die "udp relay iperf3 server did not become ready on ${TARGET_HOST}:${target_port}"

  create_allocation tcp "$target_port"
  control_id=$CREATED_ALLOCATION_ID
  control_port=$CREATED_RELAY_PORT
  create_allocation udp "$target_port"
  relay_id=$CREATED_ALLOCATION_ID
  relay_port=$CREATED_RELAY_PORT
  [[ "$control_port" == "$relay_port" ]] || die "iperf3 UDP requires matching TCP control and UDP relay ports, got tcp=${control_port} udp=${relay_port}"

  if ! run_iperf_udp_client "$relay_port" "$rate" "$duration" "$packet_size" "$client_json" "$client_log"; then
    delete_allocation "$control_id"
    delete_allocation "$relay_id"
    wait "$server_pid" 2>/dev/null || true
    return 1
  fi

  log "udp relay rate=${rate} packet_size=${packet_size} repetition=${repetition} relay_port=${relay_port} $(assert_iperf_positive "$client_json" udp)"
  if udp_loss=$(udp_lost_percent "$client_json"); then
    [[ -n "$udp_loss" ]] && log "udp relay rate=${rate} packet_size=${packet_size} repetition=${repetition} lost_percent=${udp_loss}"
  fi
  wait "$server_pid"
  delete_allocation "$control_id"
  delete_allocation "$relay_id"
  append_udp_result "$UDP_MATRIX_RESULTS" relay "$rate" "$packet_size" "$repetition" "$duration" "$client_json" "$client_log" "$server_log"
}

run_udp_matrix() {
  local -a rates packet_sizes
  local rate packet_size repetition duration matrix_dir output_prefix

  split_csv rates "$UDP_SWEEP_RATES"
  split_csv packet_sizes "$UDP_PACKET_SIZES"
  duration=$UDP_MATRIX_DURATION
  mkdir -p "${RUN_DIR}/udp-matrix"
  : >"$UDP_MATRIX_RESULTS"
  start_relayd "$TCP_SPLICE_ENABLED" "$FORCE_TCP_COPY_FALLBACK" udp-matrix "$TCP_SESSION_MODEL_ENABLED" "$TCP_SESSION_MODEL_WORKERS" "$TCP_SESSION_MODEL_SHARDED_ACCEPT" "$TCP_SESSION_MODEL_ACCEPT_BALANCED"

  for rate in "${rates[@]}"; do
    for packet_size in "${packet_sizes[@]}"; do
      for ((repetition = 1; repetition <= IPERF_REPETITIONS; repetition++)); do
        matrix_dir="${RUN_DIR}/udp-matrix/rate-${rate}/packet-${packet_size}/rep-${repetition}"
        mkdir -p "$matrix_dir"
        output_prefix="${matrix_dir}/direct"
        run_udp_direct_trial "$rate" "$packet_size" "$repetition" "$duration" "$output_prefix"
        output_prefix="${matrix_dir}/relay"
        run_udp_relay_trial "$rate" "$packet_size" "$repetition" "$duration" "$output_prefix"
      done
    done
  done

  emit_matrix_report "$UDP_MATRIX_RESULTS" "$UDP_MATRIX_SUMMARY_TXT" "$UDP_MATRIX_SUMMARY_JSON" "$UDP_SWEEP_RATES" "$UDP_PACKET_SIZES" "$IPERF_REPETITIONS" "$duration"
  stop_relayd
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
    if [[ "$IPERF_KEEP_RUN_DIR" != "1" ]]; then
      rm -rf "$RUN_DIR"
    fi
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
require_cmd "$IPERF3_BIN"
[[ -x "$RELAYD_BIN" ]] || die "expected relayd build artifact at ${RELAYD_BIN}"
validate_mode
write_manifest

case "$IPERF_MODE" in
  oneshot)
    run_one_shot_suite
    ;;
  matrix)
    run_udp_matrix
    ;;
  both)
    run_one_shot_suite
    run_udp_matrix
    ;;
esac

publish_artifacts
case "$TCP_COMPARE_MODE" in
  accept-balanced)
    refresh_tcp_accept_balanced_overall_summary "$LATEST_RUN_DIR" >"$TCP_ACCEPT_BALANCED_OVERALL_SUMMARY"
    ;;
  sharded-worker)
    refresh_tcp_sharded_worker_overall_summary "$LATEST_RUN_DIR" >"$TCP_SHARDED_WORKER_OVERALL_SUMMARY"
    ;;
  copy-vs-splice)
    refresh_tcp_copy_vs_splice_overall_summary "$LATEST_RUN_DIR" >"$TCP_COPY_VS_SPLICE_OVERALL_SUMMARY"
    ;;
  session-model)
    refresh_tcp_session_model_overall_summary "$LATEST_RUN_DIR" >"$TCP_SESSION_MODEL_OVERALL_SUMMARY"
    ;;
  workerized-session)
    refresh_tcp_workerized_session_overall_summary "$LATEST_RUN_DIR" >"$TCP_WORKERIZED_SESSION_OVERALL_SUMMARY"
    ;;
esac
log "artifacts available at ${LATEST_RUN_DIR}"
log "iperf3 e2e harness completed (${IPERF_MODE})"
