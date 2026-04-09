#!/usr/bin/env bash
set -euo pipefail

AUTH_TOKEN="${AUTH_TOKEN:-test-token}"
HTTP_LISTEN="${HTTP_LISTEN:-127.0.0.1:18080}"
PORT_RANGE="${PORT_RANGE:-18100-18120}"
SQLITE_PATH="${SQLITE_PATH:-.zig-cache/e2e/relayd-$$.sqlite3}"
TARGET_HOST="${TARGET_HOST:-127.0.0.1}"
UDP_RATE="${UDP_RATE:-100G}"
UDP_PACKET_SIZE="${UDP_PACKET_SIZE:-}"
IPERF_DURATION="${IPERF_DURATION:-2}"
IPERF_MODE="${IPERF_MODE:-oneshot}"
UDP_SWEEP_RATES="${UDP_SWEEP_RATES:-1G,5G,10G,25G,50G,100G}"
UDP_PACKET_SIZES="${UDP_PACKET_SIZES:-256,1200,1472}"
IPERF_REPETITIONS="${IPERF_REPETITIONS:-3}"
UDP_MATRIX_DURATION="${UDP_MATRIX_DURATION:-$IPERF_DURATION}"
TCP_DIRECT_VS_RELAY="${TCP_DIRECT_VS_RELAY:-1}"
TCP_BENCH_DURATION="${TCP_BENCH_DURATION:-$IPERF_DURATION}"
TCP_BENCH_REPETITIONS="${TCP_BENCH_REPETITIONS:-$IPERF_REPETITIONS}"
TCP_STREAMS="${TCP_STREAMS:-1}"
IPERF_KEEP_RUN_DIR="${IPERF_KEEP_RUN_DIR:-0}"
IPERF_SERVER_READY_TIMEOUT_SEC="${IPERF_SERVER_READY_TIMEOUT_SEC:-5}"
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
LATEST_RUN_DIR="${ARTIFACT_DIR}/iperf3-latest"
RELAYD_LOG="${RUN_DIR}/relayd.log"
TCP_SERVER_LOG="${RUN_DIR}/iperf3-tcp-server.log"
TCP_CLIENT_JSON="${RUN_DIR}/iperf3-tcp-client.json"
TCP_CLIENT_LOG="${RUN_DIR}/iperf3-tcp-client.log"
TCP_DIRECT_VS_RELAY_DIR="${RUN_DIR}/tcp-direct-vs-relay"
TCP_DIRECT_VS_RELAY_RESULTS="${RUN_DIR}/tcp-direct-vs-relay-results.ndjson"
TCP_DIRECT_VS_RELAY_SUMMARY="${RUN_DIR}/tcp-direct-vs-relay-summary.txt"
TCP_DIRECT_JSON="${RUN_DIR}/tcp-direct-client.json"
TCP_DIRECT_LOG="${RUN_DIR}/tcp-direct-client.log"
TCP_DIRECT_SERVER_LOG="${RUN_DIR}/tcp-direct-server.log"
TCP_RELAY_JSON="${RUN_DIR}/tcp-relay-client.json"
TCP_RELAY_LOG="${RUN_DIR}/tcp-relay-client.log"
TCP_RELAY_SERVER_LOG="${RUN_DIR}/tcp-relay-server.log"
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
  dump_file tcp_direct_summary "$TCP_DIRECT_VS_RELAY_SUMMARY"
  dump_file tcp_direct_client "$TCP_DIRECT_LOG"
  dump_file tcp_direct_client_json "$TCP_DIRECT_JSON"
  dump_file tcp_relay_client "$TCP_RELAY_LOG"
  dump_file tcp_relay_client_json "$TCP_RELAY_JSON"
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
  python3 - "$results_file" "$path_label" "$repetition" "$duration" "$streams" "$json_file" "$client_log" "$server_log" <<'PY'
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
payload = json.load(open(json_file, encoding='utf-8'))
end = payload.get('end') or {}
summary = end.get('sum_received') or end.get('sum') or {}
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
}
with open(results_file, 'a', encoding='utf-8') as handle:
    handle.write(json.dumps(record, sort_keys=True))
    handle.write('\n')
PY
}

emit_tcp_direct_vs_relay_summary() {
  local results_file=$1
  local summary_txt=$2
  local direct_json=$3
  local direct_log=$4
  local direct_server_log=$5
  local relay_json=$6
  local relay_log=$7
  local relay_server_log=$8
  local legacy_relay_json=$9
  local legacy_relay_log=${10}
  local legacy_relay_server_log=${11}
  python3 - "$results_file" "$summary_txt" "$direct_json" "$direct_log" "$direct_server_log" "$relay_json" "$relay_log" "$relay_server_log" "$legacy_relay_json" "$legacy_relay_log" "$legacy_relay_server_log" <<'PY'
import json
import pathlib
import shutil
import statistics
import sys

results_path = pathlib.Path(sys.argv[1])
summary_path = pathlib.Path(sys.argv[2])
direct_json_out = pathlib.Path(sys.argv[3])
direct_log_out = pathlib.Path(sys.argv[4])
direct_server_log_out = pathlib.Path(sys.argv[5])
relay_json_out = pathlib.Path(sys.argv[6])
relay_log_out = pathlib.Path(sys.argv[7])
relay_server_log_out = pathlib.Path(sys.argv[8])
legacy_relay_json_out = pathlib.Path(sys.argv[9])
legacy_relay_log_out = pathlib.Path(sys.argv[10])
legacy_relay_server_log_out = pathlib.Path(sys.argv[11])
records = [json.loads(line) for line in results_path.read_text(encoding='utf-8').splitlines() if line.strip()]
if not records:
    raise SystemExit("no TCP direct-vs-relay records found")

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
relay_records = sorted(grouped.get('relay', []), key=lambda item: item['repetition'])
if not direct_records or not relay_records:
    raise SystemExit("expected both direct and relay TCP records")

direct = summarize(direct_records)
relay = summarize(relay_records)
copy_record_artifacts(direct['representative'], direct_json_out, direct_log_out, direct_server_log_out)
copy_record_artifacts(relay['representative'], relay_json_out, relay_log_out, relay_server_log_out)
shutil.copy2(relay_json_out, legacy_relay_json_out)
shutil.copy2(relay_log_out, legacy_relay_log_out)
shutil.copy2(relay_server_log_out, legacy_relay_server_log_out)

ratio = direct['median_bps'] / relay['median_bps'] if relay['median_bps'] else None
relay_share = relay['median_bps'] / direct['median_bps'] if direct['median_bps'] else None
decision_threshold = 1.15
decision = 'splice not yet justified'
note = 'copy-vs-splice note: median relay throughput was unavailable, so the benchmark is not decision-quality.'
if ratio is not None and relay_share is not None:
    relay_share_pct = relay_share * 100.0
    if ratio >= decision_threshold:
        decision = 'splice next'
        note = (
            f"copy-vs-splice note: relay held {relay_share_pct:.1f}% of direct throughput "
            "at this 1-stream point, so copy-path overhead still looks material enough to justify a splice-focused follow-up."
        )
    else:
        note = (
            f"copy-vs-splice note: relay held {relay_share_pct:.1f}% of direct throughput "
            "at this 1-stream point, so the measured copy-path gap is not large enough to justify a splice-first follow-up yet."
        )

ratio_text = 'n/a' if ratio is None else f"{ratio:.2f}x"
lines = [
    '=== relayd tcp direct vs relay summary ===',
    f"repetitions: {direct['samples']}",
    f"duration:    {direct['duration_sec']:g}s",
    f"streams:     {direct['streams']}",
    f"decision threshold: direct/relay >= {decision_threshold:.2f}x => splice next",
    '',
    f"direct throughput: {format_decimal(direct['median_bps'], UNITS_BPS)} median / {format_decimal(direct['best_bps'], UNITS_BPS)} best",
    f"relay throughput:  {format_decimal(relay['median_bps'], UNITS_BPS)} median / {format_decimal(relay['best_bps'], UNITS_BPS)} best",
    f"direct/relay ratio: {ratio_text}",
    f"direct artifact: {direct['representative']['json_file']} | {direct['representative']['client_log']}",
    f"relay artifact:  {relay['representative']['json_file']} | {relay['representative']['client_log']}",
    note,
    decision,
]
summary_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print(summary_path.read_text(encoding='utf-8'), end='')
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

  iperf3 "${args[@]}" >"$output_json" 2>"$output_log"
}

run_iperf_tcp_client() {
  local target_port=$1
  local duration=$2
  local streams=$3
  local output_json=$4
  local output_log=$5
  local -a args

  args=(-c "$TARGET_HOST" -p "$target_port" -t "$duration" -P "$streams" -J)
  iperf3 "${args[@]}" >"$output_json" 2>"$output_log"
}

run_tcp_relay_smoke() {
  tcp_target_port=$(pick_free_port tcp)
  log "starting tcp iperf3 server on ${TARGET_HOST}:${tcp_target_port}"
  iperf3 -s -1 -B "$TARGET_HOST" -p "$tcp_target_port" >"$TCP_SERVER_LOG" 2>&1 &
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
  iperf3 -s -1 -B "$TARGET_HOST" -p "$udp_target_port" >"$UDP_SERVER_LOG" 2>&1 &
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
    run_tcp_direct_vs_relay_suite
  else
    run_tcp_relay_smoke
  fi
  run_udp_relay_smoke
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
  iperf3 -s -1 -B "$TARGET_HOST" -p "$target_port" >"$server_log" 2>&1 &
  server_pid=$!
  track_child "$server_pid"
  wait_for_tcp_listener "$TARGET_HOST" "$target_port" "$IPERF_SERVER_READY_TIMEOUT_SEC" || die "tcp direct iperf3 server did not become ready on ${TARGET_HOST}:${target_port}"

  run_iperf_tcp_client "$target_port" "$duration" "$streams" "$client_json" "$client_log"
  log "tcp direct repetition=${repetition} $(assert_iperf_positive "$client_json" tcp)"
  wait "$server_pid"
  append_tcp_result "$TCP_DIRECT_VS_RELAY_RESULTS" direct "$repetition" "$duration" "$streams" "$client_json" "$client_log" "$server_log"
}

run_tcp_relay_trial() {
  local repetition=$1
  local duration=$2
  local streams=$3
  local output_prefix=$4
  local target_port server_log client_json client_log server_pid
  local allocation_id relay_port

  target_port=$(pick_free_port tcp)
  server_log="${output_prefix}-server.log"
  client_json="${output_prefix}-client.json"
  client_log="${output_prefix}-client.log"

  log "tcp relay trial repetition=${repetition} streams=${streams} target_port=${target_port}"
  iperf3 -s -1 -B "$TARGET_HOST" -p "$target_port" >"$server_log" 2>&1 &
  server_pid=$!
  track_child "$server_pid"
  wait_for_tcp_listener "$TARGET_HOST" "$target_port" "$IPERF_SERVER_READY_TIMEOUT_SEC" || die "tcp relay iperf3 server did not become ready on ${TARGET_HOST}:${target_port}"

  create_allocation tcp "$target_port"
  allocation_id=$CREATED_ALLOCATION_ID
  relay_port=$CREATED_RELAY_PORT

  run_iperf_tcp_client "$relay_port" "$duration" "$streams" "$client_json" "$client_log"
  log "tcp relay repetition=${repetition} relay_port=${relay_port} $(assert_iperf_positive "$client_json" tcp)"
  wait "$server_pid"
  delete_allocation "$allocation_id"
  append_tcp_result "$TCP_DIRECT_VS_RELAY_RESULTS" relay "$repetition" "$duration" "$streams" "$client_json" "$client_log" "$server_log"
}

run_tcp_direct_vs_relay_suite() {
  local repetition trial_dir output_prefix

  mkdir -p "$TCP_DIRECT_VS_RELAY_DIR"
  : >"$TCP_DIRECT_VS_RELAY_RESULTS"

  for ((repetition = 1; repetition <= TCP_BENCH_REPETITIONS; repetition++)); do
    trial_dir="${TCP_DIRECT_VS_RELAY_DIR}/rep-${repetition}"
    mkdir -p "$trial_dir"
    output_prefix="${trial_dir}/direct"
    run_tcp_direct_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix"
    output_prefix="${trial_dir}/relay"
    run_tcp_relay_trial "$repetition" "$TCP_BENCH_DURATION" "$TCP_STREAMS" "$output_prefix"
  done

  emit_tcp_direct_vs_relay_summary \
    "$TCP_DIRECT_VS_RELAY_RESULTS" \
    "$TCP_DIRECT_VS_RELAY_SUMMARY" \
    "$TCP_DIRECT_JSON" \
    "$TCP_DIRECT_LOG" \
    "$TCP_DIRECT_SERVER_LOG" \
    "$TCP_RELAY_JSON" \
    "$TCP_RELAY_LOG" \
    "$TCP_RELAY_SERVER_LOG" \
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
  iperf3 -s -1 -B "$TARGET_HOST" -p "$target_port" >"$server_log" 2>&1 &
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
  iperf3 -s -1 -B "$TARGET_HOST" -p "$target_port" >"$server_log" 2>&1 &
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
require_cmd iperf3
[[ -x "$RELAYD_BIN" ]] || die "expected relayd build artifact at ${RELAYD_BIN}"
validate_mode
write_manifest

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
log "artifacts available at ${LATEST_RUN_DIR}"
log "iperf3 e2e harness completed (${IPERF_MODE})"
