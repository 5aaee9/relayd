#!/usr/bin/env python3
"""Probe a relay listener through its public TCP or UDP data-plane seam."""

import argparse
import socket


def receive_exact(stream: socket.socket, size: int) -> bytes:
    chunks = []
    remaining = size
    while remaining:
        chunk = stream.recv(remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def tcp_probe(host: str, port: int, payload: bytes, expected_size: int, timeout: float) -> bytes:
    with socket.create_connection((host, port), timeout=timeout) as stream:
        stream.settimeout(timeout)
        stream.sendall(payload)
        stream.shutdown(socket.SHUT_WR)
        return receive_exact(stream, expected_size)


def udp_probe(
    host: str, port: int, payload: bytes, timeout: float, count: int, send_only: bool
) -> list[bytes]:
    family, socktype, protocol, _, address = socket.getaddrinfo(
        host,
        port,
        type=socket.SOCK_DGRAM,
    )[0]
    with socket.socket(family, socktype, protocol) as stream:
        stream.settimeout(timeout)
        for _ in range(count):
            stream.sendto(payload, address)
        if send_only:
            return []
        return [stream.recvfrom(65535)[0] for _ in range(count)]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("protocol", choices=("tcp", "udp"))
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    parser.add_argument("label")
    parser.add_argument("payload")
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--send-only", action="store_true")
    parser.add_argument("--accept-any-response", action="store_true")
    args = parser.parse_args()

    if args.count < 1:
        parser.error("--count must be at least 1")
    if args.protocol == "tcp" and args.count != 1:
        parser.error("--count is only supported for UDP probes")
    if args.protocol == "tcp" and args.send_only:
        parser.error("--send-only is only supported for UDP probes")

    payload = args.payload.encode("utf-8")
    expected = f"{args.label}:{args.payload}".encode("utf-8")
    if args.protocol == "tcp":
        responses = [
            tcp_probe(args.host, args.port, payload, len(expected), args.timeout)
        ]
    else:
        responses = udp_probe(
            args.host,
            args.port,
            payload,
            args.timeout,
            args.count,
            args.send_only,
        )

    for response in responses:
        if args.accept_any_response and not response:
            raise SystemExit("peer returned no response")
        if not args.accept_any_response and response != expected:
            raise SystemExit(
                f"unexpected response: expected={expected!r}, actual={response!r}"
            )
    print("\n".join(response.decode("utf-8") for response in responses))


if __name__ == "__main__":
    main()
