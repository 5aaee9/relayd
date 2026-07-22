import json
import re
import shlex
import statistics
import time


def shell_join(arguments):
    return " ".join(shlex.quote(str(argument)) for argument in arguments)


def api(machine, method, path, payload=None, expected_status=200, authorization="bearer"):
    command = [
        "curl",
        "--silent",
        "--show-error",
        "--request",
        method,
        "--write-out",
        "\\n%{http_code}",
    ]
    if authorization == "bearer":
        command.extend(["--header", f"Authorization: Bearer {AUTH_TOKEN}"])
    elif authorization is not None:
        command.extend(["--header", f"Authorization: {authorization}"])
    if payload is not None:
        command.extend(
            [
                "--header",
                "Content-Type: application/json",
                "--data",
                json.dumps(payload, separators=(",", ":")),
            ]
        )
    command.append(f"http://{API_HOST}:{API_PORT}{path}")
    output = machine.succeed(shell_join(command))
    body, status_text = output.rsplit("\n", 1)
    status = int(status_text.strip())
    if expected_status is not None:
        t.assertEqual(
            status,
            expected_status,
            f"{method} {path}: expected HTTP {expected_status}, got {status}; body={body!r}",
        )
    return status, body


def api_json(machine, method, path, payload=None, expected_status=200):
    _, body = api(machine, method, path, payload, expected_status)
    try:
        return json.loads(body)
    except json.JSONDecodeError as error:
        raise AssertionError(f"{method} {path} returned invalid JSON: {body!r}") from error


def create_allocation(machine, protocol):
    allocation = api_json(
        machine,
        "POST",
        "/v1/allocations",
        {"protocol": protocol},
        201,
    )
    t.assertEqual(allocation["protocol"], protocol)
    t.assertIsInstance(allocation["port"], int)
    t.assertTrue(allocation["id"])
    return allocation


def put_binding(machine, allocation_id, host, target_port):
    binding = api_json(
        machine,
        "PUT",
        f"/v1/allocations/{allocation_id}/binding",
        {"host": host, "target_port": target_port},
        200,
    )
    t.assertEqual(binding["allocation_id"], allocation_id)
    t.assertEqual(binding["host"], host)
    t.assertEqual(binding["target_port"], target_port)
    wait_for_active(machine, allocation_id)
    return binding


def create_bound(machine, protocol, host, target_port):
    allocation = create_allocation(machine, protocol)
    put_binding(machine, allocation["id"], host, target_port)
    return allocation


def wait_for_active(machine, allocation_id, timeout=15):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        status, body = api(
            machine,
            "GET",
            f"/v1/allocations/{allocation_id}/binding",
            expected_status=None,
        )
        if status == 200:
            last = json.loads(body)
            if last.get("runtime_status") == "active":
                return last
        time.sleep(0.25)
    raise AssertionError(f"allocation {allocation_id} did not become active: {last!r}")


def probe(machine, protocol, port, label, payload, host=None):
    relay_host = host or RELAY_HOST4
    return machine.succeed(
        shell_join(
            [
                "relayd-probe",
                protocol,
                relay_host,
                port,
                label,
                payload,
                "--timeout",
                "2",
            ]
        )
    ).strip()


def udp_burst_probe(machine, port, label, payload, count, host=None):
    relay_host = host or RELAY_HOST4
    return machine.succeed(
        shell_join(
            [
                "relayd-probe",
                "udp",
                relay_host,
                port,
                label,
                payload,
                "--timeout",
                "5",
                "--count",
                count,
                "--send-only",
            ]
        )
    ).strip()


def expect_probe_failure(machine, protocol, port, host=None):
    relay_host = host or RELAY_HOST4
    machine.fail(
        shell_join(
            [
                "relayd-probe",
                protocol,
                relay_host,
                port,
                "unreachable",
                "must-fail",
                "--timeout",
                "1",
                "--accept-any-response",
            ]
        )
    )


def prometheus(machine):
    command = [
        "curl",
        "--silent",
        "--show-error",
        "--fail",
        "--header",
        f"Authorization: Bearer {AUTH_TOKEN}",
        "--write-out",
        "\\n%{content_type}",
        f"http://{API_HOST}:{API_PORT}/metrics",
    ]
    output = machine.succeed(shell_join(command))
    body, content_type = output.rsplit("\n", 1)
    t.assertEqual(content_type.strip(), "text/plain; version=0.0.4; charset=utf-8")
    return body


def prometheus_value(text, metric, port, protocol):
    pattern = re.compile(
        rf"^{re.escape(metric)}\{{port=\"{port}\",protocol=\"{protocol}\"\}} ([0-9.eE+-]+)$",
        re.MULTILINE,
    )
    match = pattern.search(text)
    if not match:
        raise AssertionError(
            f"missing Prometheus sample {metric} port={port} protocol={protocol}:\n{text}"
        )
    return float(match.group(1))


def iperf_sample(machine, host, port, protocol):
    command = [
        "iperf3",
        "--client",
        host,
        "--port",
        port,
        "--json",
        "--omit",
        "1",
        "--time",
        "2",
    ]
    if protocol == "tcp":
        command.extend(["--parallel", "2"])
    else:
        command.extend(["--udp", "--bitrate", "20M", "--length", "1200"])
    raw = machine.succeed(shell_join(command))
    result = json.loads(raw)
    end = result.get("end") or {}
    summary = end.get("sum_received") or end.get("sum") or {}
    bits_per_second = float(summary.get("bits_per_second") or 0.0)
    byte_count = int(summary.get("bytes") or 0)
    if bits_per_second <= 0 or byte_count <= 0:
        raise AssertionError(
            f"invalid iperf3 {protocol} result for {host}:{port}: "
            f"bytes={byte_count}, bits_per_second={bits_per_second}"
        )
    return {
        "bits_per_second": bits_per_second,
        "bytes": byte_count,
        "lost_percent": float(summary.get("lost_percent") or 0.0),
    }


def median(values):
    return float(statistics.median(values))
